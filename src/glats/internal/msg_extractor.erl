-module(msg_extractor).
-export([extract_dict/1, decode_message/1, decode_request_message/1]).

%% Extract the dict from {msg, Dict} tuple
extract_dict({_MsgAtom, Dict}) ->
    {ok, Dict};
extract_dict(Other) ->
    {error, {not_tuple, Other}}.

%% Decode entire message directly, handling correct atom keys (for subscriptions)
decode_message({_MsgAtom, Dict}) when is_map(Dict) ->
    %% Use the correct atom keys (lowercase, not Elixir format)
    Topic = maps:get(topic, Dict, ""),
    Body = maps:get(body, Dict, ""),
    Sid = maps:get(sid, Dict, -1),
    ReplyTo = case maps:get(reply_to, Dict, nil) of
        nil -> none;
        RT -> {some, RT}
    end,
    Status = case maps:get(status, Dict, nil) of
        nil -> none;
        S -> {some, S}
    end,
    Headers = maps:get(headers, Dict, #{}),
    
    {ok, {raw_message, Sid, Status, Topic, Headers, ReplyTo, Body}};
decode_message(Other) ->
    {error, {invalid_format, Other}}.

decode_request_message(Dict) when is_map(Dict) ->
    Topic = maps:get(topic, Dict, "topic_not_found"),
    Body = maps:get(body, Dict, "body_not_found"),
    ReplyTo = case maps:get(reply_to, Dict, nil) of
        nil -> nil;
        RT -> RT
    end,
    Headers = maps:get(headers, Dict, #{}),
    
    {ok, #{
        topic => Topic,
        body => Body,
        reply_to => ReplyTo,
        headers => Headers
    }};
decode_request_message(Other) ->
    {error, {invalid_request_format, Other}}.
