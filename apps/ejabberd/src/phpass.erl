-module(phpass).

-compile(export_all).

-record(hasher, {
            itoa64 = <<"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz">>,
            iteration_count_log2,
            portable_hashes,
            algorithm = none
        }).

-record(blowfish, {
            in,
            out = <<>>,
            n = 0,
            c1,
            itoa64 = <<"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789">>
        }).

-spec hash_init(integer(), boolean(), none|blowfish|edes) -> #hasher{}.
hash_init(ICL2, PH, Algorithm) when ICL2 < 4 orelse ICL2 > 31 ->
    hash_init(8, PH, Algorithm);
hash_init(IterationCountLog2, PortableHashes, Algorithm) ->
    #hasher{
            iteration_count_log2 = IterationCountLog2,
            portable_hashes = PortableHashes,
            algorithm = Algorithm
        }.

gensalt_private(Input, Hasher) ->
    Pos = erlang:min(Hasher#hasher.iteration_count_log2 + 5, 30),
    ItoaFrag = binary:at(Hasher#hasher.itoa64, Pos),

    Encoded64 = encode64:encode(Input, 6, Hasher#hasher.itoa64),

    <<"$P$", ItoaFrag, Encoded64/binary>>.

crypt_private(_Pass, <<$$, C, $$, _/binary>>, _Hasher) when C /= $P andalso C /= $H ->
    <<"*0">>;
crypt_private(Pass, Setting, #hasher{ itoa64 = Itoa64 }) ->
    Output = case Setting of
        <<"*0", _/binary>> -> <<"*1">>;
        _ -> <<"*0">>
    end,

    Char = binary:at(Setting, 3),
    case binary:match(Itoa64, <<Char>>) of
        {CL2, _} when CL2 < 7 orelse CL2 > 30 orelse byte_size(Setting) < 12 ->
            Output;
        {CountLog2, _} ->
            Count = 1 bsl CountLog2,

            <<_:4/binary, Salt:8/binary, _/binary>> = Setting,
            Hash = do_hash(crypto:md5(<<Salt/binary, Pass/binary>>), Pass, Count),
            <<FinalOut1:12/binary, _/binary>> = Setting,
            Encoded = encode64:encode(Hash, 16, Itoa64),
            <<FinalOut1/binary, Encoded/binary>>
    end.

do_hash(Hash, _Pass, 0) ->
    Hash;
do_hash(Hash, Pass, N) ->
    do_hash(crypto:md5(<<Hash/binary, Pass/binary>>), Pass, N-1).

gensalt_extended(Input, #hasher{ itoa64 = Itoa64 }) ->
    encode64:encode(Input, 3, Itoa64).

gensalt_blowfish(Input) ->
    gensalt_blowfish_1(#blowfish{in = Input}).

gensalt_blowfish_1(#blowfish{ in = Input, out = Output0, n = N0, itoa64 = Itoa64 } = Blowfish) ->
    C1 = binary:at(Input, N0),
    O1 = binary:at(Itoa64, C1 bsr 2),
    C1b = (C1 band 16#03) bsl 4,
    gensalt_blowfish_2(Blowfish#blowfish{ out = <<Output0/binary, O1>>, n = N0+1, c1 = C1b}).

gensalt_blowfish_2(#blowfish{out = Output0, itoa64 = Itoa64, c1 = C1, n = N}) when N >= 16 ->
    O1 = binary:at(Itoa64, C1),
    <<Output0/binary, O1>>;
gensalt_blowfish_2(#blowfish{ n = N, in = Input, out = Output0, itoa64 = Itoa64, c1 = C1} = Blowfish) ->
    C2 = binary:at(Input, N),
    C1b = C1 bor (C2 bsr 4),
    O1 = binary:at(Itoa64, C1b),

    gensalt_blowfish_3(Blowfish#blowfish{ n = N+1, c1 = (C2 band 16#0f) bsl 2, out = <<Output0/binary, O1>>}).

gensalt_blowfish_3(#blowfish{ in = Input, n = N, c1 = C1, out = Output0, itoa64 = Itoa64 } = Blowfish) ->
    C2 = binary:at(Input, N),
    C1b = C1 bor (C2 bsr 6),
    O1 = binary:at(Itoa64, C1b),
    O2 = binary:at(Itoa64, C2 band 16#3f),

    gensalt_blowfish_1(Blowfish#blowfish{ n = N+1, out = <<Output0/binary, O1, O2>> }).

hash_password(Pass, #hasher{ algorithm = Algo, portable_hashes = Port } = Hasher)
        when Algo == none orelse Port == true ->
    Random = crypto:strong_rand_bytes(6),
    Hash = crypt_private(Pass, gensalt_private(Random, Hasher), Hasher),
    verify_hash(Hash, 34);
hash_password(Pass, #hasher{ algorithm = blowfish } = Hasher) ->
    Random = crypto:strong_rand_bytes(16),
    Hash = php_crypto(Pass, gensalt_blowfish(Random), Hasher),
    verify_hash(Hash, 60);
hash_password(Pass, #hasher{ algorithm = edes } = Hasher) ->
    Random = crypto:strong_rand_bytes(3),
    Hash = php_crypto(Pass, gensalt_extended(Random, Hasher), Hasher),
    verify_hash(Hash, 20).

php_crypto(Data, Salt, #hasher{ iteration_count_log2 = ICL2, algorithm = blowfish }) ->
    IterCount = 1 bsl ICL2,

    Hash = do_crypt(Data, Salt, IterCount, blowfish_ecb_encrypt),

    O1 = $0 + ICL2 div 10,
    O2 = $0 + ICL2 rem 10, 
    <<"$2a$", O1, O2, "$", Salt/binary, Hash/binary>>;
php_crypto(Data, Salt, #hasher{ iteration_count_log2 = ICL2, algorithm = edes, itoa64 = Itoa64 }) ->
    CountLog2 = erlang:min(ICL2 + 8, 24),
    IterCount = (1 bsl CountLog2) - 1,

    Hash = do_crypt(Data, Salt, IterCount, des_ecb_encrypt),

    O1 = binary:at(Itoa64, IterCount band 16#3f),
    O2 = binary:at(Itoa64, (IterCount bsr 6) band 16#3f),
    O3 = binary:at(Itoa64, (IterCount bsr 12) band 16#3f),
    O4 = binary:at(Itoa64, (IterCount bsr 18) band 16#3f),

    <<"_", O1, O2, O3, O4, Salt/binary, Hash/binary>>.

do_crypt(Data, _Salt, 0, _Algo) ->
    Data;
do_crypt(Data, Salt, N, Algo) ->
    do_crypt(crypto:Algo(Salt, Data), Salt, N-1, Algo).

verify_hash(Hash, Len) when byte_size(Hash) == Len ->
    Hash;
verify_hash(Hash, _Len) ->
    Hash.

check_password(Pass, StoredHash, #hasher{itoa64 = Itoa64} = Hasher) ->
    case crypt_private(Pass, StoredHash, Hasher) of
        <<"*", _/binary>> ->
            case StoredHash of
                <<"_", O1, O2, O3, O4, Salt:4/binary, Hash/binary>> ->
                    {V1, _} = binary:match(Itoa64, <<O1>>),
                    {V2, _} = binary:match(Itoa64, <<O2>>),
                    {V3, _} = binary:match(Itoa64, <<O3>>),
                    {V4, _} = binary:match(Itoa64, <<O4>>),
                    IterCount = V1 + (V2 bsl 6) + (V3 bsl 12) + (V4 bsl 18),
                    do_crypt(Pass, Salt, IterCount, des_ecb_encrypt) =:= Hash;
                <<"$2a$", O1, O2, "$", Salt:22/binary, Hash/binary>> ->
                    V1 = (O1 - $0) * 10,
                    V2 = O2 - $0,
                    IterCount = 1 bsl (V1+V2),
                    do_crypt(Pass, Salt, IterCount, blowfish_ecb_encrypt) =:= Hash
            end;
        Hash ->
            Hash =:= StoredHash
    end.

