import rue, asyncio, uuid, time, json

time.sleep(1)

db = str(uuid.uuid4())

def mid(it):
    return list(map(lambda i : i.id if i is not None else None, it))

def mit(it):
    return list(map(lambda i : i.item if i is not None else None, it))

async def to_list(ait):
    r = []
    async for i in ait:
        r.append(i)
    return r

async def main():
    queue = rue.Queue(db)
    await queue.create(rue.DefaultOptions(max_tries = 2))
    # make sure that queue.check() works
    await queue.check()

    # Queue some items
    it1 = await queue.new("it1", "cool_pipeline", "me")
    it2 = await queue.new("it2", "cool_pipeline", "me", priority = 1)
    it3 = await queue.new("it3", "cool_pipeline", "me")
    it4 = await queue.new("it4", "boring_pipeline", "me")

    assert it4.attempts == []

    # Does pending work correctly?
    pending = await queue.pending()
    assert mid(pending) == mid([it1, it3, it4, it2]), mit(pending)

    # Does counts work correctly?
    counts = await queue.counts()
    assert counts.limbo == 0
    assert counts.counts == {
        "cool_pipeline": {rue.Status.TODO: 3, rue.Status.CLAIMED: 0, rue.Status.STASHED: 0},
        "boring_pipeline": {rue.Status.TODO: 1, rue.Status.CLAIMED: 0, rue.Status.STASHED: 0},
    }, counts

    # Are items claimed as expected?
    expected = [it1, it3, it2, None, it4]
    real = []
    args = (
        ("me", "cool_pipeline"),
        ("me", "cool_pipeline"),
        ("me", "cool_pipeline"),
        ("me", "cool_pipeline"),
        ("me", "boring_pipeline")
    )
    for a in args:
        res = await queue.claim(*a)
        if res:
            assert res.claimed_at is not None
        real.append(res)
    assert mid(expected) == mid(real), mit(real)

    # Do results work correctly?
    await queue.store_result(it4, 0, {"foo": "bar"}, "type")
    l: list[rue.JobResult] = []
    async for result in queue.get_results(it4):
        l.append(result)
    assert len(l) == 1, l
    assert l[0].type == "type" and l[0].data['foo'] == "bar", l

    # maxtries is 2, so first failure should requeue
    r1 = await queue.fail(it4, "Too boring", rue.AttemptId(0))
    assert not r1.admin_log
    assert r1.current_attempt() == 0
    assert r1.attempt_count() == 1
    attempt = r1.attempts[r1.current_attempt()]
    assert attempt.error == "Too boring"
    assert attempt.pipeline == "me"
    assert attempt.pipeline_version is None
    assert r1.status == rue.Status.TODO, r1
    # claim again so that try counter is increased
    it4_new = await queue.claim("me", "boring_pipeline", "1.0")
    assert it4_new and it4_new.id == it4.id, it4_new
    assert it4_new.attempts[it4_new.current_attempt()].pipeline_version == "1.0"
    # second failure should move to error
    r2 = await queue.fail(it4, "Too boring", it4_new.current_attempt(), is_poke = True)
    assert r2.status == rue.Status.ERROR, r2
    assert len(r2.admin_log) == 1 \
        and r2.admin_log[0].action == ("fail", it4_new.current_attempt()) \
        and r2.admin_log[0].reason == "Too boring" \
        and abs(time.time() - r2.admin_log[0].time.timestamp()) < 10
    # and finishing should work nonetheless
    assert (await queue.finish(it4)).status == rue.Status.DONE

    # Check dripfeeding
    await queue.set_dripfeed_behaviour("a_stash", 2)
    dri = await queue.new("dri", "cool_pipeline", "me", stash = "a_stash")
    dri2 = await queue.new("dri2", "cool_pipeline", "me", stash = "a_stash")
    dri3 = await queue.new("dri3", "cool_pipeline", "me", stash = "a_stash")
    # Ensures that they have not yet been added to the queue
    assert not await queue.claim("me", "cool_pipeline")
    expected = [dri, dri2]
    items = await queue.dripfeed()
    assert set(mid(items)) == set(mid(expected)), mit(items)
    assert mid(await queue.pending()) == mid(expected)

    # We haven't claimed anything, so this should do nothing
    items = await queue.dripfeed()
    assert not items, mit(items)

    # Claim dri
    dri = await queue.claim("me", "cool_pipeline")
    # Make sure that allow_retries = False works
    ndri = await queue.fail(dri, "Failed", 0, rue.RetryBehaviour.NEVER)
    assert ndri.status == rue.Status.ERROR

    # Make sure we get the new item in the queue
    expected = [dri3]
    items = await queue.dripfeed()
    assert mid(items) == mid(expected), mit(items)
    assert mid(await queue.pending()) == mid([dri2, dri3])

asyncio.run(main())
