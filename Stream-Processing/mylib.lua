#!lua name=mylib

local function add_wc(keys, args)
    local temp = args[2]
    local dict = cjson.decode(temp)
    local status = redis.call("XACK", args[4], args[3], args[5])
    if status == 1 then
        for x,y in pairs(dict) do
            redis.call("ZINCRBY", args[1], y, x)
        end        
        local t = redis.call("TIME")
        local epoch = math.floor(t[1] * 1000 + t[2] / 1000);
        local lat = tostring(epoch-args[6]);
        redis.call("HSET","latency",args[5],lat)

    end
    return "OK"
end

redis.register_function('add_wc', add_wc)