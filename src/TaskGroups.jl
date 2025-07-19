module TaskGroups

import DataStructures
import Dates

using Base: Semaphore, acquire, release, wait
using Base.Threads
using ScopedValues

export TaskGroup, submit_job!, wait_group, JobTreeJoin

include("futures.jl")

struct TaskGroupJob
    # We use (user-provided-priority, submit-timestamp) as the priority, to
    # encourage LIFO behaviour when many tasks are submitted with identical
    # priorities.
    priority::Tuple{Int,Float64}
    sf::ScopedFunctor
    future::Future
    begun_flag::Threads.Atomic{Bool}
end

Base.isless(a::TaskGroupJob, b::TaskGroupJob) = a.priority < b.priority

default_num_workers() = max(256, 4*Threads.nthreads())

# Simple worker pool to avoid creating hundreds of thousands of Julia Task
# objects. A TaskGroup allows up to max_workers simultaneous Julia tasks,
# processing a priority queue of jobs specified as Julia closures. If there
# are fewer than max_workers tasks when a new job is submitted via
# submit_job!(), a new task is created; if a task finishes a job and finds
# the job queue empty, it exits.
mutable struct TaskGroup
    name::String
    cond::Threads.Condition
    pq::Vector{TaskGroupJob}
    # This can temporarily exceed `max_workers` due to the scoped_release
    # mechanism that is needed to prevent deadlocks when tasks depend on
    # each other.
    num_active_workers::Int
    num_workers::Int
    max_workers::Int
    throttle::Semaphore

    # NB if you specify max_queued_jobs, calls to submit_job!() may block
    # until the job queue is < max_queued_jobs. So you must never create
    # jobs that depend on the Future of a job they themselves will spawn,
    # or deadlock may result.
    function TaskGroup(
        name::String;
        max_workers=default_num_workers(),
        max_queued_jobs=typemax(Int),
    )
        new(
            name,
            Threads.Condition(),
            Vector{TaskGroupJob}(),
            0,
            0,
            max_workers,
            Semaphore(max_queued_jobs),
        )
    end
end

function not_too_busy(tg::TaskGroup)
    @lock tg.cond begin
        # Each worker task should have a few jobs in queue, but not too many
        return tg.num_workers + length(tg.pq) < 4*tg.max_workers
    end
end

function set_max_workers!(tg::TaskGroup, max_workers)
    @lock tg.cond begin
        _set_max_workers_nolock!(tg, max_workers)
    end
end
function _set_max_workers_nolock!(tg::TaskGroup, max_workers)
    @assert tg.num_active_workers == 0 "Can't modify max workers while active workers exist"
    tg.max_workers = max_workers
    return nothing
end

function get_max_workers(tg::TaskGroup)
    @lock tg.cond begin
        return tg.max_workers
    end
end

# Smaller priority runs sooner.
function submit_job!(
    f::Function,
    tg::TaskGroup,
    priority=1000
) where {T}
    acquire(tg.throttle)
    sf = ScopedFunctor(f)
    begun_flag = Threads.Atomic{Bool}(false)

    # submit_job!() returns a Future for the result. If the user demands
    # the result of the Future, and the job hasn't started running, this
    # fetch_action() hook will run sf() in the calling task.
    function fetch_action(future)
        _try_run(tg, sf, future, begun_flag)
    end

    fut = Future{Any}(; fetch_action)
    job = TaskGroupJob((priority, time()), sf, fut, begun_flag)
    @lock tg.cond begin
        DataStructures.heappush!(tg.pq, job)
        _maybe_spawn_worker(tg)
    end
    return fut
end

function _maybe_spawn_worker(tg::TaskGroup)
    @assert islocked(tg.cond)
    if tg.num_active_workers < tg.max_workers && !isempty(tg.pq)
        tg.num_active_workers += 1
        tg.num_workers += 1
        @spawn _worker_task(tg)
    end
    return nothing
end

function _worker_task(tg::TaskGroup)
    while true
        # Get next job
        job = @lock tg.cond begin
            @assert tg.num_active_workers <= tg.max_workers

            # Shutdown if there are no more jobs
            if isempty(tg.pq)
                tg.num_active_workers -= 1
                tg.num_workers -= 1
                notify(tg.cond; all=false)
                return nothing
            end

            DataStructures.heappop!(tg.pq)
        end

        # Run the job, if it hasn't been started already
        _try_run(tg, job.sf, job.future, job.begun_flag)
    end
end

function _try_run(
    tg::TaskGroup,
    sf::ScopedFunctor,
    future::Future,
    begun_flag::Threads.Atomic{Bool}
)
    if Threads.atomic_cas!(begun_flag, false, true) == false
        # Allow another job to be submitted
        release(tg.throttle)

        try
            future[] = sf()
        catch e
            setexception!(future, e)
            @warn "TaskGroup $(tg.name): a job threw an exception"
        end
    end
end

# Intended for testing or shutdowns.
function wait_for(
    predicate::Function;
    timeout::Union{Nothing, Dates.Period} = nothing,
    interval::Dates.Period=Dates.Millisecond(1),
    what="predicate"
)
    interval_sec = interval / Dates.Second(1)
    timeout_sec = timeout === nothing ? nothing : (timeout / Dates.Second(1))
    start = time()
    while !predicate()
        if timeout_sec !== nothing && time() - start ≥ timeout_sec
            @warn "Timeout waiting for $(what) after waiting for $(timeout)."
            return false
        end
        # lint-disable-next-line: `sleep`
        sleep(interval_sec)
    end
    return true
end

# Intended for testing or shutdowns. Usage on hot path needs a
# faster implementation first.
function wait_group(tg::TaskGroup, timeout_s::Union{Int,Nothing}=nothing)::Nothing
    timeout = timeout_s === nothing ? nothing : Dates.Second(timeout_s)
    wait_for(; timeout, what="TaskGroup $(tg.name)", interval=Dates.Millisecond(100)) do
        @lock tg.cond begin
            tg.num_active_workers == 0
        end
    end
    nothing
end

function wait_group(f::Function, tg::TaskGroup, timeout_s::Union{Int,Nothing}=nothing)::Bool
    timeout = timeout_s === nothing ? nothing : Dates.Second(timeout_s)
    if (wait_for(;timeout, what="TaskGroup $(tg.name)", interval=Dates.Millisecond(100)) do
            @lock tg.cond begin
                tg.num_active_workers == 0
            end
        end)
        f()
        return true
    else
        return false
    end
end

"""
    JobTreeJoin

A utility to wait on a tree of jobs where the root is the first submitted
job. Similar to the `@sync` macro, but works for `TaskGroup`s. All
descendant jobs must be submitted through submit_job!(f, tg, jtj).
"""
mutable struct JobTreeJoin
    cond::Threads.Condition
    counter::Int
    futures::Vector{Future{Any}}

    JobTreeJoin() = new(Threads.Condition(), 0, Vector{Future}())
end

function submit_job!(f::Function, tg::TaskGroup, jtj::JobTreeJoin, priority=1000)
    @lock jtj.cond begin
        jtj.counter += 1
        fut = submit_job!(tg, priority) do
            try
                f()
            finally
                @lock jtj.cond begin
                    jtj.counter -= 1
                    jtj.counter == 0 && notify(jtj.cond; all=true)
                end
            end
        end
        push!(jtj.futures, fut)
        return fut
    end
end

"""
    Base.wait(jtj::JobTreeJoin)

Block until all jobs in the `JobTreeJoin` have completed.
"""
function Base.wait(jtj::JobTreeJoin)
    @lock jtj.cond begin
        while jtj.counter > 0
            wait(jtj.cond)
        end
    end
    wait_until_done(jtj.futures)
end

"""
    Base.length(jtj::JobTreeJoin)

Return the number of jobs in the tree.
"""
function Base.length(jtj::JobTreeJoin)
    return @lock jtj.cond length(jtj.futures)
end

end # module
