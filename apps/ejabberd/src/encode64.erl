-module(encode64).

-compile(export_all).

-record(state, {
        in,
        out = <<>>,
        n = 0,
        value,
        count,
        itoa64}).

encode(Input, Count, Itoa64) ->
    step0(#state{
            in = Input,
            count = Count,
            itoa64 = Itoa64}).

step0(#state{in = Input, n = N} = State) ->
    step1(State#state{ n = N + 1, value = binary:at(Input, N) }).

step1(#state{out = Output, itoa64 = Itoa64, value = Value} = State) ->
    Output1 = binary:at(Itoa64, Value band 16#3f),
    step2(State#state{ out = <<Output/binary, Output1>> }).

step2(#state{in = Input, value = Value, n = N, count = Count} = State) when N < Count ->
    step3(State#state{ value = Value bor (binary:at(Input, N) bsl 8) });
step2(State) ->
    step3(State).

step3(#state{out = Output, itoa64 = Itoa64, value = Value, n = N} = State) ->
    Output1 = binary:at(Itoa64, (Value bsr 6) band 16#3f),
    step4(State#state{ out = <<Output/binary, Output1>>, n = N+1}).

step4(#state{out = Output, n = N, count = Count}) when N-1 >= Count ->
    Output;
step4(#state{n = N, count = Count, value = Value, in = Input} = State) when N < Count ->
    step5(State#state{ value = Value bor (binary:at(Input, N) bsl 16) });
step4(State) ->
    step5(State).

step5(#state{out = Output, itoa64 = Itoa64, value = Value, n = N} = State) ->
    Output1 = binary:at(Itoa64, (Value bsr 12) band 16#3f),
    step6(State#state{ out = <<Output/binary, Output1>>, n = N + 1}).

step6(#state{n = N, count = Count, out = Output}) when N-1 >= Count ->
    Output;
step6(#state{out = Output, itoa64 = Itoa64, value = Value} = State) ->
    Output1 = binary:at(Itoa64, (Value bsr 18) band 16#3f),
    step7(State#state{ out = <<Output/binary, Output1>> }).

step7(#state{n = N, count = Count} = State) when N < Count ->
    step0(State);
step7(#state{out = Output}) ->
    Output.
