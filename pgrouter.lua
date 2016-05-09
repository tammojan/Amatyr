-- 
-- A simple API adapter for using postgresql internal subreq in a reusable manner
--
-- Copyright Tor Hveem <thveem> 2013
--

local cjson = require "cjson"

-- Load our API commands
local api = require "myapi"

-- mapping patterns to query views
local routes = {
    ['max']               = api.bla, --api.max,
    ['record/([a-z_]+)/([a-z]+)']= api.bla, --api.record,
    ['year/([0-9]{4})']   = api.bla, --api.year,
    ['now']               = api.now,
    ['day']               = api.bla, --api.day,
    ['recent']            = api.bla, --api.recent,
    ['station']           = api.bla, --api.meta,
    ['windhist']          = api.bla, --api.windhist,
    ['hour']              = api.hour, --api.by_dateunit,
    ['(month)']           = api.bla, --api.by_dateunit,
}
-- Set the content type
ngx.header.content_type = 'application/json';
-- Our URL base, must match location in nginx config
local BASE = '/api/'
-- iterate route patterns and find view
for pattern, view in pairs(routes) do
    local uri = '^' .. BASE .. pattern
    local match = ngx.re.match(ngx.var.uri, uri, "oj") -- regex mather in compile mode
    if match then
        local ret, exit = view(match) 
        -- Detect JSONP
        local callback = ngx.req.get_uri_args()['callback']
        if callback then
            ret = callback .. '(' .. ret .. ');'
        end
        -- Allow CORS
        ngx.header['Access-Control-Allow-Origin'] = '*';
        -- Print the returned res
        ngx.print(ret)
        -- If not given exit, then assume OK
        if not exit then exit = ngx.HTTP_OK end
        -- Exit with returned exit value
        ngx.exit( exit )
    end
end
-- no match, return 404
ngx.exit( ngx.HTTP_NOT_FOUND )
