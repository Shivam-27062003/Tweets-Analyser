#!lua name=mylib

local function sum_up(keys,args)
    local temp = args[2]
    local dict = cjson.decode(temp)
    local status = redis.call("XACK", args[4], args[3], args[5])
    if status == 1 then
        for x,y in pairs(dict) do
            redis.call("ZINCRBY", args[1], y, x)
        end        
    end
    return "OK"
end
redis.register_function('sum_up', sum_up)
