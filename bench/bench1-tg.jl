# JULIA_NUM_THREADS=4

using TaskGroups

const completion_count = Threads.Atomic{Int}(0)

function foo(spawn_seq::Int, z::UInt64)
    h = UInt64(z)
    try
        t0 = time()
        yield_times = Vector{Float64}()

        # Do some busy work, yield frequently
        for i=1:100000000
            h = hash(i, h)
            if mod(i,1000000) == 0
               t1 = time()
               yield()
               t2 = time()
               push!(yield_times, (t2-t1))
            end
        end
        completion_seq = 1+Threads.atomic_add!(completion_count, 1)
        span = time() - t0
        sort!(yield_times, rev=true)
        print("$(spawn_seq),$(completion_seq),$(span),$(yield_times)\n")
    catch e
        showerror(stderr, e, catch_backtrace())
        rethrow()
    end
    return h
end

function bar(N::Int)
    tg = TaskGroup("bench1")
    jtj = JobTreeJoin()
    u = UInt64(0)
    for j in 1:N
        arg = j
        submit_job!(tg, jtj) do
            foo(arg, u)
        end
        u = hash(j, u)
        sleep(1/N)
    end
    wait(jtj)
end

stats = @timed bar(10000)
println("stats = $(stats)")
flush(stdout)
