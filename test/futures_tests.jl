@testset "Future" begin
    future = Future{Int}(() -> 0)
    @test future[] == 0
    @test done(future)

    future = Future() do
        return 10+1
    end
    @test future[] == 11

    future = Future{Int}()
    @test !done(future)
    future[] = 11
    @test done(future)
    @test future[] == 11

    future = Future{Int}() do
        throw(KeyError(3))
    end
    threw = begin
        try
            future[]
            false
        catch
            true
        end
    end
    @test threw
    @test done(future)

    future = Future{Int}(3)
    @test future[] == 3
end
