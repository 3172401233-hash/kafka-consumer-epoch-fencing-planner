import test from 'node:test';
import assert from 'node:assert/strict';
import { OwnershipStateMachine } from '../src/state-machine.js';

test('stale generation commits are fenced', () => {
  const sm = new OwnershipStateMachine();
  sm.join({ memberId: 'A', generation: 1, partitions: ['p0'] });
  sm.join({ memberId: 'B', generation: 2, partitions: ['p0'] });

  const stale = sm.commit({ memberId: 'A', generation: 1, partition: 'p0', offset: 41 });
  assert.equal(stale.allowed, false);
  assert.equal(stale.reason, 'stale-generation');

  const fresh = sm.commit({ memberId: 'B', generation: 2, partition: 'p0', offset: 42 });
  assert.equal(fresh.allowed, true);

  const report = sm.report();
  assert.equal(report.holders.p0, 'B');
  assert.equal(report.illegalCommits.length, 1);
  assert.equal(report.illegalCommits[0].memberId, 'A');
});

test('duplicate join is idempotent and produces no extra takeovers', () => {
  const sm = new OwnershipStateMachine();
  const first = sm.join({ memberId: 'A', generation: 3, partitions: ['p0', 'p1'] });
  const second = sm.join({ memberId: 'A', generation: 3, partitions: ['p0', 'p1'] });

  assert.equal(first.duplicate, false);
  assert.equal(second.duplicate, true);
  assert.deepEqual(second.assigned, []);

  const report = sm.report();
  assert.equal(report.takeoverOrder.length, 2);
  assert.equal(report.holders.p0, 'A');
  assert.equal(report.holders.p1, 'A');

  const commit = sm.commit({ memberId: 'A', generation: 3, partition: 'p0', offset: 7 });
  assert.equal(commit.allowed, true);
});

test('late revocation does not strip the new owner', () => {
  const sm = new OwnershipStateMachine();
  sm.join({ memberId: 'A', generation: 1, partitions: ['p0'] });
  // Rebalance: p0 moves to B at generation 2.
  sm.join({ memberId: 'B', generation: 2, partitions: ['p0'] });
  // The revocation for A at generation 1 arrives late.
  const result = sm.revoke({ memberId: 'A', generation: 1, partitions: ['p0'] });

  assert.deepEqual(result.revoked, []);
  assert.equal(result.ignored.length, 1);
  assert.equal(result.ignored[0].currentOwner, 'B');

  const report = sm.report();
  assert.equal(report.holders.p0, 'B');

  // A's commit is fenced; B's commit is allowed.
  assert.equal(sm.commit({ memberId: 'A', generation: 2, partition: 'p0', offset: 1 }).allowed, false);
  assert.equal(sm.commit({ memberId: 'B', generation: 2, partition: 'p0', offset: 1 }).allowed, true);
});

test('timely revocation releases the partition', () => {
  const sm = new OwnershipStateMachine();
  sm.join({ memberId: 'A', generation: 1, partitions: ['p0'] });
  const result = sm.revoke({ memberId: 'A', generation: 1, partitions: ['p0'] });

  assert.deepEqual(result.revoked, ['p0']);
  assert.equal(sm.report().holders.p0, null);
  assert.equal(
    sm.commit({ memberId: 'A', generation: 1, partition: 'p0', offset: 5 }).reason,
    'not-owner',
  );
});

test('producer epoch regression fences the commit and aborts the transaction', () => {
  const sm = new OwnershipStateMachine();
  sm.join({ memberId: 'A', generation: 1, partitions: ['p0'] });

  const begin = sm.beginTransaction({
    memberId: 'A', generation: 1, partition: 'p0', txnId: 't1', producerEpoch: 5,
  });
  assert.equal(begin.accepted, true);

  // Epoch moves backwards: 5 -> 3.
  const regressed = sm.commit({
    memberId: 'A', generation: 1, partition: 'p0', offset: 10, txnId: 't1', producerEpoch: 3,
  });
  assert.equal(regressed.allowed, false);
  assert.equal(regressed.reason, 'epoch-regression');

  const report = sm.report();
  assert.equal(report.abortedTransactions.length, 1);
  assert.equal(report.abortedTransactions[0].reason, 'epoch-regression');
  assert.equal(report.illegalCommits[0].reason, 'epoch-regression');

  // A begin with a regressed epoch is rejected outright.
  const beginStale = sm.beginTransaction({
    memberId: 'A', generation: 1, partition: 'p0', txnId: 't2', producerEpoch: 2,
  });
  assert.equal(beginStale.accepted, false);
  assert.equal(beginStale.reason, 'epoch-regression');
});

test('transaction commit race: only the current owner commits', () => {
  const sm = new OwnershipStateMachine();
  sm.join({ memberId: 'A', generation: 1, partitions: ['p0'] });
  sm.beginTransaction({ memberId: 'A', generation: 1, partition: 'p0', txnId: 'txA', producerEpoch: 1 });

  // Rebalance hands p0 to B; A's open transaction is aborted by the takeover.
  sm.join({ memberId: 'B', generation: 2, partitions: ['p0'] });
  sm.beginTransaction({ memberId: 'B', generation: 2, partition: 'p0', txnId: 'txB', producerEpoch: 2 });

  // A (old owner, old generation) loses the race.
  const loser = sm.commit({
    memberId: 'A', generation: 1, partition: 'p0', offset: 100, txnId: 'txA', producerEpoch: 1,
  });
  assert.equal(loser.allowed, false);
  assert.equal(loser.reason, 'stale-generation');

  // B (current owner, current generation) wins.
  const winner = sm.commit({
    memberId: 'B', generation: 2, partition: 'p0', offset: 100, txnId: 'txB', producerEpoch: 2,
  });
  assert.equal(winner.allowed, true);

  // Re-committing the same transaction is rejected.
  const replay = sm.commit({
    memberId: 'B', generation: 2, partition: 'p0', offset: 101, txnId: 'txB', producerEpoch: 2,
  });
  assert.equal(replay.allowed, false);
  assert.equal(replay.reason, 'transaction-already-committed');

  const report = sm.report();
  assert.deepEqual(
    report.abortedTransactions.map((t) => t.txnId),
    ['txA'],
  );
  assert.equal(report.abortedTransactions[0].reason, 'ownership-takeover');
  assert.equal(report.holders.p0, 'B');
});

test('member crash releases partitions and aborts open transactions', () => {
  const sm = new OwnershipStateMachine();
  sm.join({ memberId: 'A', generation: 1, partitions: ['p0'] });
  sm.join({ memberId: 'B', generation: 1, partitions: ['p1'] });
  sm.beginTransaction({ memberId: 'A', generation: 1, partition: 'p0', txnId: 'txA', producerEpoch: 1 });

  const crashed = sm.crash({ memberId: 'A' });
  assert.deepEqual(crashed.released, ['p0']);

  const report = sm.report();
  assert.equal(report.holders.p0, null);
  assert.equal(report.holders.p1, 'B');
  assert.equal(report.abortedTransactions.length, 1);
  assert.equal(report.abortedTransactions[0].reason, 'member-crashed');

  // Commits from the crashed member are illegal.
  const commit = sm.commit({ memberId: 'A', generation: 1, partition: 'p0', offset: 9 });
  assert.equal(commit.allowed, false);

  // Takeover order records assign then crash-release in order.
  const causes = report.takeoverOrder.map((t) => `${t.partition}:${t.cause}`);
  assert.deepEqual(causes, ['p0:assign', 'p1:assign', 'p0:crash']);
});

test('takeover order reflects the full ownership chain', () => {
  const sm = new OwnershipStateMachine();
  sm.join({ memberId: 'A', generation: 1, partitions: ['p0'] });
  sm.join({ memberId: 'B', generation: 2, partitions: ['p0'] });
  sm.revoke({ memberId: 'B', generation: 2, partitions: ['p0'] });
  sm.join({ memberId: 'C', generation: 3, partitions: ['p0'] });

  const chain = sm.report().takeoverOrder.map((t) => [t.from, t.to]);
  assert.deepEqual(chain, [[null, 'A'], ['A', 'B'], ['B', null], [null, 'C']]);
  assert.equal(sm.report().holders.p0, 'C');
});
