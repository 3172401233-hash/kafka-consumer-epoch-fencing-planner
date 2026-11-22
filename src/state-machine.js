/**
 * Ownership state machine for a Kafka-style consumer group.
 *
 * The machine consumes events (join, revoke, begin/commit transaction,
 * offset commit, crash) and emits *decisions* only: it never talks to a
 * real broker. Every decision is deterministic given the event sequence.
 *
 * Fencing rules:
 *  - A commit carrying a generation older than the current group
 *    generation is rejected (stale-generation).
 *  - A commit from a member that does not currently hold the partition
 *    is rejected (not-owner).
 *  - A transactional commit whose producer epoch is lower than the last
 *    epoch seen for that member is rejected (epoch-regression) and its
 *    transaction is aborted.
 *  - A revocation event that arrives after ownership has already moved
 *    to a new member at a higher generation is ignored (late-revocation)
 *    and must not strip the new owner.
 */
export class OwnershipStateMachine {
  constructor() {
    this.groupGeneration = 0;
    /** memberId -> { generation, status } */
    this.members = new Map();
    /** partition -> { owner, generation } */
    this.partitions = new Map();
    /** memberId -> highest producer epoch seen */
    this.producerEpochs = new Map();
    /** partition -> open transaction */
    this.openTransactions = new Map();
    /** committed transaction ids */
    this.committedTransactions = new Set();

    this.illegalCommits = [];
    this.abortedTransactions = [];
    this.takeoverOrder = [];
    this.ignoredRevocations = [];
    this.commits = [];
  }

  #partitionState(partition) {
    if (!this.partitions.has(partition)) {
      this.partitions.set(partition, { owner: null, generation: 0 });
    }
    return this.partitions.get(partition);
  }

  #recordTakeover(partition, from, to, generation, cause) {
    this.takeoverOrder.push({ partition, from, to, generation, cause });
  }

  #abortTransaction(partition, reason) {
    const open = this.openTransactions.get(partition);
    if (!open) return null;
    this.openTransactions.delete(partition);
    const abort = { partition, txnId: open.txnId, memberId: open.memberId, reason };
    this.abortedTransactions.push(abort);
    return abort;
  }

  /**
   * A member joins (or re-joins) the group with an assignment.
   * Duplicate joins with the same generation are idempotent.
   */
  join({ memberId, generation, partitions = [] }) {
    if (generation < this.groupGeneration) {
      return { accepted: false, reason: 'stale-generation', generation: this.groupGeneration };
    }
    this.groupGeneration = generation;

    const existing = this.members.get(memberId);
    const duplicate = existing !== undefined
      && existing.status === 'active'
      && existing.generation === generation;
    this.members.set(memberId, { generation, status: 'active' });

    const taken = [];
    for (const partition of partitions) {
      const state = this.#partitionState(partition);
      if (state.owner === memberId && state.generation === generation) {
        continue; // idempotent re-join: no duplicate takeover entries
      }
      const previous = state.owner;
      if (previous && previous !== memberId) {
        // New owner fences any transaction the old owner still has open.
        this.#abortTransaction(partition, 'ownership-takeover');
      }
      state.owner = memberId;
      state.generation = generation;
      this.#recordTakeover(partition, previous, memberId, generation, 'assign');
      taken.push(partition);
    }
    return { accepted: true, duplicate, assigned: taken };
  }

  /**
   * A revocation event for a member. Late events (the partition has since
   * been reassigned at a higher generation) are ignored and must not
   * revoke the new owner's ownership.
   */
  revoke({ memberId, generation, partitions = [] }) {
    const revoked = [];
    const ignored = [];
    for (const partition of partitions) {
      const state = this.#partitionState(partition);
      const stillOwner = state.owner === memberId;
      const notSuperseded = state.generation <= generation;
      if (stillOwner && notSuperseded) {
        this.#abortTransaction(partition, 'ownership-revoked');
        state.owner = null;
        this.#recordTakeover(partition, memberId, null, generation, 'revoke');
        revoked.push(partition);
      } else {
        const entry = {
          partition,
          memberId,
          generation,
          currentOwner: state.owner,
          currentGeneration: state.generation,
        };
        this.ignoredRevocations.push(entry);
        ignored.push(entry);
      }
    }
    return { revoked, ignored };
  }

  /**
   * Open a transaction on a partition. A higher producer epoch fences any
   * transaction currently open by another member on the same partition.
   */
  beginTransaction({ memberId, generation, partition, txnId, producerEpoch }) {
    const state = this.#partitionState(partition);
    if (generation < this.groupGeneration) {
      return { accepted: false, reason: 'stale-generation' };
    }
    if (state.owner !== memberId) {
      return { accepted: false, reason: 'not-owner' };
    }
    const lastEpoch = this.producerEpochs.get(memberId);
    if (lastEpoch !== undefined && producerEpoch < lastEpoch) {
      return { accepted: false, reason: 'epoch-regression' };
    }
    const open = this.openTransactions.get(partition);
    if (open && open.memberId !== memberId) {
      const otherEpoch = this.producerEpochs.get(open.memberId) ?? -1;
      if (producerEpoch > otherEpoch) {
        this.#abortTransaction(partition, 'fenced-by-higher-epoch');
      } else {
        return { accepted: false, reason: 'transaction-already-open' };
      }
    }
    this.producerEpochs.set(memberId, Math.max(producerEpoch, lastEpoch ?? producerEpoch));
    this.openTransactions.set(partition, { txnId, memberId, epoch: producerEpoch, generation });
    return { accepted: true, txnId };
  }

  /**
   * Commit offsets, optionally inside a transaction.
   * Returns a decision describing whether the commit is allowed.
   */
  commit({ memberId, generation, partition, offset, txnId = null, producerEpoch = null }) {
    const state = this.#partitionState(partition);
    const reject = (reason) => {
      const illegal = { memberId, generation, partition, offset, txnId, reason };
      this.illegalCommits.push(illegal);
      return { allowed: false, reason };
    };

    if (generation < this.groupGeneration) {
      return reject('stale-generation');
    }
    if (state.owner !== memberId) {
      return reject('not-owner');
    }
    const member = this.members.get(memberId);
    if (!member || member.status !== 'active') {
      return reject('member-not-active');
    }

    if (txnId !== null) {
      if (this.committedTransactions.has(txnId)) {
        return reject('transaction-already-committed');
      }
      const open = this.openTransactions.get(partition);
      if (!open || open.txnId !== txnId || open.memberId !== memberId) {
        return reject('transaction-not-open');
      }
      const lastEpoch = this.producerEpochs.get(memberId);
      if (producerEpoch !== null && lastEpoch !== undefined && producerEpoch < lastEpoch) {
        this.#abortTransaction(partition, 'epoch-regression');
        return reject('epoch-regression');
      }
      this.openTransactions.delete(partition);
      this.committedTransactions.add(txnId);
    }

    const record = { memberId, generation, partition, offset, txnId };
    this.commits.push(record);
    return { allowed: true, commit: record };
  }

  /**
   * A member crashes: its partitions are released and its open
   * transactions are aborted. Later commits from it are illegal.
   */
  crash({ memberId }) {
    const member = this.members.get(memberId);
    if (member) member.status = 'crashed';
    const released = [];
    for (const [partition, state] of this.partitions) {
      if (state.owner === memberId) {
        this.#abortTransaction(partition, 'member-crashed');
        state.owner = null;
        this.#recordTakeover(partition, memberId, null, state.generation, 'crash');
        released.push(partition);
      }
    }
    return { released };
  }

  /** Deterministic snapshot of every decision the machine has produced. */
  report() {
    const holders = {};
    for (const [partition, state] of [...this.partitions.entries()].sort()) {
      holders[partition] = state.owner;
    }
    return {
      groupGeneration: this.groupGeneration,
      holders,
      illegalCommits: [...this.illegalCommits],
      abortedTransactions: [...this.abortedTransactions],
      takeoverOrder: [...this.takeoverOrder],
      ignoredRevocations: [...this.ignoredRevocations],
      commits: [...this.commits],
    };
  }
}
