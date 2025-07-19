using Base.Threads
using Suppressor

@testset "TaskGroup and JobTreeJoin" begin
    counter = Threads.Atomic{Int}(0)
    tg = TaskGroup("Test")
    jtj = JobTreeJoin()
    submit_job!(tg, jtj) do
        submit_job!(tg, jtj) do
            sleep(0.3)
            Threads.atomic_add!(counter, 1)
        end
        sleep(0.1)
        atomic_add!(counter, 1)
    end
    wait(jtj)
    @test length(jtj) == 2
    @test counter[] == 2
end

@testset "TaskGroup and exceptions in jobs" begin
    counter = Threads.Atomic{Int}(0)
    tg = TaskGroup("Test")
    jtj = JobTreeJoin()
    err_log = @capture_err begin
        try
            submit_job!(tg, jtj) do
                throw("error")
            end
            wait(jtj)
        catch e
            println(stderr, e)
        end
    end
    @test contains(err_log, "error")
end

@testset "TaskGroups with ScopedValues" begin
    using ScopedValues
    S = ScopedValue(1)
    counter = Threads.Atomic{Int}(0)
    tg = TaskGroup("Test")
    jtj = JobTreeJoin()

    # test multiple levels of nesting while changing the scope variable
    submit_job!(tg, jtj) do
        @test S[] == 1
        with() do
            submit_job!(tg, jtj) do
                @test S[] == 1
                with(S => 10) do
                    submit_job!(tg, jtj) do
                        @test S[] == 10
                        with(S => 100) do
                            submit_job!(tg, jtj) do
                                @test S[] == 100
                                with(S => 1000) do
                                    submit_job!(tg, jtj) do
                                        @test S[] == 1000
                                        sleep(0.1)
                                        Threads.atomic_add!(counter, S[])
                                    end
                                end
                                with(S => 10000) do
                                    submit_job!(tg, jtj) do
                                        @test S[] == 10000
                                        sleep(0.1)
                                        Threads.atomic_add!(counter, S[])
                                    end
                                end
                                with(S => 100000) do
                                    submit_job!(tg, jtj) do
                                        @test S[] == 100000
                                        sleep(0.1)
                                        Threads.atomic_add!(counter, S[])
                                    end
                                end
                                sleep(0.1)
                                Threads.atomic_add!(counter, S[])
                            end
                        end
                        sleep(0.1)
                        Threads.atomic_add!(counter, S[])
                    end
                end
                sleep(0.1)
                Threads.atomic_add!(counter, S[])
            end
        end
        sleep(0.1)
        Threads.atomic_add!(counter, S[])
    end

    wait(jtj)
    @test counter[] == (1 + 1 + 10 + 100 + 1000 + 10000 + 100000)
end
