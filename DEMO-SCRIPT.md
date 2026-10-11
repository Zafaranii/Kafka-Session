# Demo script — Event Streaming using Apache Kafka

For the presenters. Four short acts, each right after the deck section it
illustrates, about 20 minutes in total. Each act has one job: to make one
idea from the slides *visible*, so the audience doesn't have to take it on
trust.

Every number below was measured on this stack (MacBook, Docker with 8 GB).
Expect yours to be in the same range, not identical.

## Before the session

1. `scripts/prepare-demo.sh` (about 2 minutes). It must end with `Ready.`
   This wipes the Kafka log and Postgres. Act 4 depends on the topic
   holding only what you send during the session.
2. Open two tabs: **http://localhost:8090** (demo) and
   **http://localhost:8090/kafka.html** (Inside Kafka). Zoom so each
   section fits the projector.
3. Check on Inside Kafka: green dot, `3/3 brokers up`, every partition's ISR
   is `{1, 2, 3}`, and email-sender shows `0 running`.
4. Don't run a rehearsal load test after preparing. If you do, run the
   script again.

| After deck § | Act | Minutes |
|---|---|---|
| 02 The Solution | 1 · The broker holds the message | ~7 |
| 03 Introducing Kafka | 2 · A record has an address | ~2.5 |
| 04 Architecture | 3 · Kill a broker | ~4 |
| 05 Data Flow | 4 · A new subscriber joins late | ~5 |

**If you run long, cut:** in Act 3, the wait for leadership to return to
broker 2; in Act 4, the 4th instance (idle consumer).

---

## Act 1 · The broker holds the message — after §02 (slides 8–10)

**Point:** async moves work off the user's critical path, and the broker
*holds* events when a consumer is down. Both shown, not claimed.

1. **Send** once on each panel. Show that both deliver. On the event-driven
   panel the journey's Kafka step reads like `Kafka · ali → P0 #0`. Don't
   explain it yet ("we'll come back to that P0").
2. **Ask:** "If the service that pushes to the browser dies, what happens to
   a notification sent meanwhile?"
3. **Stop ws-service**, then send on the sync panel. The retries fail and the
   message is lost: the 502 tells the user it failed. Gotcha: the
   DB row *was* written before the push failed, so the system now disagrees
   with itself.
4. **Stop notifier-consumer**, then send 3 on the event-driven panel. Every
   send gets **202**. Point at the badge:
   `Waiting in Kafka → notifier-consumer 3 · db-sink 0`.
   - **Say:** "Three events are sitting in Kafka for this consumer. The
     database consumer already has them: same topic, independent groups."
5. **Start notifier-consumer.** The 3 notifications arrive and the badge
   returns to 0 within about 2 s.
6. **Run Load Test** with 10,000 requests and **100 in flight** (the
   default). This also fills the topic for Act 4. The test is run by the
   `load-runner` service, not the browser, and it plays the *business
   service* from the deck. On the sync side it calls notify-api and waits
   for the whole chain (slide 4). On the event-driven side it publishes
   straight to Kafka, like slide 9, and waits for that message's own acks=all
   acknowledgement. Same number in flight, same warm-up, and delivery is
   measured the same way on both sides.
   - **If someone asks what "in flight" means:** how many requests are
     waiting for an answer at the same moment. With 100, the script sends
     100, and each time one gets its answer it sends the next, until all
     10,000 are done. The two paths don't run at the same time: sync goes
     first, then event-driven, so neither slows the other down. Each also
     starts with 200 warm-up requests that aren't counted.
   - **What each side waits for:** sync waits for notify-api's 200 OK, which
     only comes after the DB write and the push are done. Event-driven waits
     for Kafka to confirm it has the message on at least 2 of the 3 brokers.
     The DB write and the push happen afterwards; their times are still
     measured, but nobody waits for them.
   - Measured at 100 in flight: caller blocked p50 about 265–300 ms (sync)
     vs about 20–50 ms (event-driven), 0 failures on either side.
   - **Say:** "On the event-driven side the script *is* the business service,
     publishing directly, like on slide 9. And we didn't make the work
     disappear: the DB write and the push still happen, they just stopped
     being the caller's problem."
   - **Optional, slide 5 moment:** run it again with **500 in flight**. The
     sync path starts failing (in testing, more than half the calls): 500
     callers fight over db-service's 10 database connections and time out.
     Kafka takes all 10,000. That's "database choke points" and "no traffic
     buffer", live.

**If it goes wrong:** if the badge says `kafka-inspector unreachable`,
carry on. The Inside Kafka page shows the same lag later.

## Act 2 · A record has an address — after §03 (slides 12, 14, 18)

**Point:** the log is append-only. An offset is a position. The key picks
the partition. Reading moves a pointer and deletes nothing.

Switch to **Inside Kafka**, top two sections.

1. **Ask:** "If I send three notifications to Ali, where do they go?"
2. Click **ali · ×5**. The record card fills in with key `"ali"`, the value,
   header `trace_id`, partition, offset and timestamp. That's slide 14,
   live. The five boxes appear at the right end of **Partition 0** with
   consecutive offsets.
3. Click **omar ×1** (lands in P1) and **sara ×1** (P2). Then **ali ×1**
   again, which goes to P0 again.
   - **Say:** "Same key, same partition, so Ali's notifications stay in
     order. Different people can land in different partitions."
4. Point at the coloured **pointers** under each lane: every group sits at
   `next…`. Then point at the counts: each partition holds thousands of
   records from Act 1's load test. **Reset Load Test Data** on the demo page
   deletes those rows from Postgres, but the log keeps every record. Act 4
   relies on that.
   - **Say:** "Consuming doesn't remove anything. A group just remembers how
     far it got."

## Act 3 · Kill a broker — after §04 (slides 16, 19)

**Point:** replication is why Kafka isn't the new single point of failure.

Scroll to **Brokers**. Each card shows its partitions as Leader (dark) or
Follower; P0, P1 and P2 each have their leader on a different broker.

1. **Ask:** "We put everything through Kafka. What if Kafka dies?"
2. **Stop broker 2.** In about 3 s its card turns red and its replicas show
   offline. Its partition's leadership moves to a surviving broker and the
   lane header reads `ISR {1, 3}`.
3. Click **Send 20 notifications** right under the cards. All 20 come back
   **202**. Measured: 360 out of 360 sends accepted while each broker in turn
   was stopped.
   - **Say:** "Every write still went to two in-sync copies. That's
     `acks=all` with `min.insync.replicas=2`."
4. **Start broker 2.** It rejoins the ISR within about 5 s. Within about
   15–20 s it takes its leadership back (preferred leader rebalancing).
5. **Say the caveat:** "Three containers on one laptop is a simulation. Real
   clusters put brokers on separate machines and racks."

**Hold for Q&A:** stopping a *second* broker. The page asks you to confirm.
Writes are then refused: only one copy is left, which is below the minimum
of 2, and the controllers lose their quorum. **Say:** "Kafka chooses not to
lose data over staying writable." Start both again afterwards and wait for
`3/3 brokers up`.

**If it goes wrong:** the header may say "some values are a few seconds old
while the cluster recovers". That's normal for a few seconds after a stop.
If a button seems stuck, wait about 10 s (Docker's stop timeout) before
clicking again.

## Act 4 · A new subscriber joins late — after §05 (slides 8, 9, 22–24)

**Point:** adding a subscriber doesn't touch the producer. Retention lets
it start from history. Partitions cap parallelism. Replay repeats side
effects.

Scroll to **Consumer groups**. db-sink has 3 members with 1 partition each
(slide 23's *partitions = consumers*). notifier-consumer has 1 member with
3 partitions (*partitions > consumers*). email-sender has never run.

1. **Ask:** "Product wants an email for every notification. On the sync side
   that means changing notify-api. Here?"
2. Click **+** (start email-sender #1). It starts at offset 0, so it reads
   **all** the history: about 10,000 records waiting. Its pointers show
   `email-sender N behind` on the log. It processes about **160–170
   records/s**. The delay is *simulated* (5 ms per email), and the card says
   so.
   - **Say:** "We changed nothing on the producer. A new group with a new
     group.id simply started reading."
3. Click **+** twice (3 instances). After about 3 s of **rebalancing…**,
   each member owns one partition. The total rate roughly doubles to
   triples. In the test, P1 had fewer records, so its instance finished
   first and sat idle while the others kept going.
   - **Say:** "Partitions are the unit of parallelism. One partition, one
     consumer, at any time."
4. Click **+** once more (4 instances). One member shows **idle**: four
   consumers, three partitions (slide 23).
5. Click **−**. The group rebalances and the remaining members take over
   (about 3 s).
6. **Slide 24 warning, using what just happened:** "It just 'emailed'
   everyone from the load test, including people who were notified an hour
   ago. Replay re-runs side effects. Make consumers idempotent."

**Optional rewind** (only if time allows; terminal, run with email-sender
stopped, i.e. click − until `0 running`):

```bash
docker exec notification-demo-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 --group email-sender \
  --topic notification-requests --reset-offsets --to-earliest --execute
```

Lag jumps back to the full topic size; start an instance to re-read it all.

---

## Expected pushback, honest answers

- **"You just moved the work."** Yes. Total work is the same. The difference
  is who waits for it (no longer the user) and what breaks when one part is
  down (one consumer group, not the whole request).
- **"Just add a queue/retry to the sync side."** You can. Make that queue
  durable, replicated, replayable and readable by several independent
  consumers, and you have rebuilt the broker.
- **"Isn't async slower end-to-end?"** It can be. The claim is that the
  *response* no longer depends on downstream work, not that delivery is
  faster.
- **"The Kafka side skipped the API, isn't that unfair?"** It's what slide 9
  shows: backend services publish to the broker directly. producer-api only
  exists so a browser can send one notification. The Kafka side is also not
  allowed to batch-and-forget: every event waits for its own acks=all
  acknowledgement, just like every sync call waits for its own 200.
- **"What about duplicates?"** Kafka consumers are at-least-once. The DB sink
  upserts on `request_id`, so a redelivered record overwrites the same row.
  The email-sender doesn't dedupe, which Act 4's replay shows.

## Reset between rehearsals

`scripts/prepare-demo.sh`, every time. It's the only way to get an empty
topic and an email-sender group that has never run.
