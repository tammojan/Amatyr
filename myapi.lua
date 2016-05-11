local cjson = require "cjson"
local ngx = ngx
local string = string
local io = require "io"
local assert = assert
local mysql = require "resty.mysql"
local conf

module(...)

if not conf then
    local f = assert(io.open(ngx.var.document_root .. "/etc/config.json", "r"))
    local c = f:read("*all")
    f:close()

    conf = cjson.decode(c)
end

-- Translate front end column names to back end column names
local function column(key)
    return conf.db.columns[key]
end

local function dbreq(sql)
    local db = mysql:new()
    db:set_timeout(30000)
    local ok, err = db:connect(
        {
            host=conf.db.host,
            port=conf.db.port,
            database=conf.db.database,
            user=conf.db.user,
            password=conf.db.password,
            compact_arrays=false
        })
    if not ok then
        ngx.say(err)
    end
    local res, err = db:query(sql)
    if not res then
        ngx.log(ngx.ERR, 'Failed SQL query:' .. sql)
        res = {error=err}
    end
    db:set_keepalive(0,10)
    return cjson.encode(res)
end

function bla()
    return "[]"
end

-- Helper function to get a start argument and return SQL constrains
local function getDateConstrains(startarg, interval)
    local where = ''
    local andwhere = ''
    if startarg then
        local start
        local endpart = "1 YEAR"
        if string.upper(startarg) == 'TODAY' then
            start = "CURRENT_DATE"
            endpart = "1 DAY"
        elseif string.lower(startarg) == 'yesterday' then
            start = "DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY), '%Y-%m-%d')"
            endpart = '1 DAY'
        elseif string.upper(startarg) == '3DAY' then
            start = "DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 3 DAY)"
            endpart = '3 DAY'
        elseif string.upper(startarg) == 'WEEK' then
            start = "DATE_SUB(CURRENT_DATE, INTERVAL 1 WEEK)"
            endpart = '1 WEEK'
        elseif string.upper(startarg) == '7DAYS' then
            start = "DATE_SUB(CURRENT_DATE, INTERVAL 1 WEEK)"
            endpart = '1 WEEK'
        elseif string.upper(startarg) == 'MONTH' then
            start = "DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)"
            endpart = "1 MONTH"
        elseif string.upper(startarg) == 'YEAR' then
            start = "DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-01-01')"
            endpart = "1 YEAR"
        elseif string.upper(startarg) == 'ALL' then
            start = "DATE '1900-01-01'" -- Should be old enough :-)
            endpart = "200 YEAR"
        else
            start = "DATE '" .. startarg .. "'"
        end
        -- use interval if provided, if not use the default endpart
        if not interval then
            interval = endpart
        end

        local wherepart = [[
        (
            timestamp BETWEEN ]]..start..[[
            AND
            DATE_ADD(]]..start..[[, INTERVAL ]]..endpart..[[)
        )
        ]]
        where = 'WHERE ' .. wherepart
        andwhere = 'AND ' .. wherepart
    end
    return where, andwhere
end

--- Return weather data by hour, week, month, year, whatever..
function by_dateunit(match)
    local unit = 'hour'
    if match[1] then
        if match[1] == 'month' then
            unit = 'day'
        end
    elseif ngx.req.get_uri_args()['start'] == 'month' then
        unit = 'day'
    end
    -- get the date constraints
    local where, andwhere = getDateConstrains(ngx.req.get_uri_args()['start'])
    local sql = dbreq([[
    SELECT
        ]]..datetrunc(unit)..[[ AS datetime,
        AVG(]]..column('outtemp')..[[) as outtemp,
        MIN(]]..column('outtemp')..[[) as tempmin,
        MAX(]]..column('outtemp')..[[) as tempmax,
        AVG(]]..column('dewpoint')..[[) as dewpoint,
        AVG(]]..column('windspeed')..[[) as windspeed,
        AVG(]]..column('winddir')..[[) as winddir,
        AVG(]]..column('humidity')..[[) as outhumidity
    FROM ]]..conf.db.table..[[ as a
    ]]..where..[[
    GROUP BY datetime
    ORDER BY datetime
    ]])
    return sql
end

-- Convert timezone of timestamp, truncates to dateunit
function datetrunc(dateunit)
    if dateunit == 'minute' then
        return [[DATE_FORMAT(CONVERT_TZ(timestamp,'UTC','Europe/Amsterdam'),"%Y-%m-%d %H:%i:00")]]
    elseif dateunit == 'second' then
        return [[DATE_FORMAT(CONVERT_TZ(timestamp,'UTC','Europe/Amsterdam'),"%Y-%m-%d %H:%i:00")]]
    elseif dateunit == 'year' then
        return [[DATE_FORMAT(CONVERT_TZ(timestamp,'UTC','Europe/Amsterdam'),"%Y-01-01")]]
    elseif dateunit == 'hour' then
        return [[DATE_FORMAT(CONVERT_TZ(timestamp,'UTC','Europe/Amsterdam'),"%Y-%m-%d %H:00:00")]]
    elseif dateunit == 'day' then
        return [[DATE_FORMAT(CONVERT_TZ(timestamp,'UTC','Europe/Amsterdam'),"%Y-%m-%d 00:00:00")]]
    end
    return [[CONVERT_TZ(timestamp,'UTC','Europe/Amsterdam')]]
end

function recent()
    return dbreq([[select ]]..datetrunc('minute')..[[ AS datetime, AVG(temp) AS outtemp, AVG(windspeed) AS windspeed FROM ]] .. conf.db.table .. [[ WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR) GROUP BY datetime ORDER BY datetime DESC;]])
end

function now()
    return dbreq([[
    SELECT 
        CONVERT_TZ(timestamp,'UTC','Europe/Amsterdam') AS datetime,
        ]]..column('windspeed')..[[ AS windspeed,
        ]]..column('winddir')..[[ AS winddir,
        ]]..column('dewpoint')..[[ AS dewpoint,
        ]]..column('outtemp')..[[ AS outtemp,
        ]]..column('humidity')..[[ AS outhumidity
     FROM ]] .. conf.db.table .. [[ ORDER BY timestamp DESC LIMIT 1;]])
end

function day(match)
    local where, andwhere = getDateConstrains(ngx.req.get_uri_args()['start'])
    local sql = dbreq([[
    SELECT
        ]]..datetrunc('minute')..[[ AS datetime,
        AVG(]]..column('outtemp')..[[) as outtemp,
        AVG(]]..column('dewpoint')..[[) as dewpoint,
        MIN(]]..column('windspeed')..[[) as windspeed,
        AVG(]]..column('winddir')..[[) as winddir,
        AVG(]]..column('humidity')..[[) as outhumidity
    FROM ]]..conf.db.table..[[
    ]]..where..[[
    GROUP BY datetime
    ORDER BY datetime
    ]])
    return sql
end

function windhist(match)
    local where, andwhere = getDateConstrains(ngx.req.get_uri_args()['start'])
    return dbreq([[
        SELECT FLOOR(COUNT(*)) AS count,
        CASE WHEN ]]..column('windspeed')..[[<2.0 THEN NULL ELSE (ROUND(]]..column('winddir')..[[/10,0)*10) END as d, 
        AVG(]]..column('windspeed')..[[)*1.94384449 AS avg
        FROM ]]..conf.db.table..[[
        ]]..where..[[
        GROUP BY d
        ORDER BY d
    ]])
end

-- Function to return extremeties from database, min/maxes for different time intervals
function record(match)

    local key = match[1]
    local func = string.upper(match[2])
    local period = string.upper(ngx.req.get_uri_args()['start'])
    local where, andwhere = getDateConstrains(period)
    local sql

    if func == 'SUM' then
        -- The SUM part doesn't need the datetime of the record since the datetime is effectively over the whole scope
        sql = [[
            SELECT
            SUM(]]..column(key)..[[) AS ]]..key..[[
            FROM ]]..conf.db.table..[[
            ]]..where..[[
        ]]
    else
        sql = [[
        SELECT
            ]]..datetrunc('')..[[ AS datetime,
            TIMESTAMPDIFF(DAY, timestamp, NOW()) AS age,
            ]]..column(key)..[[ AS ]]..key..[[
        FROM ]]..conf.db.table..[[
        WHERE
        ]]..column(key)..[[ =
        (
            SELECT
                ]]..func..[[(]]..column(key)..[[)
            FROM ]]..conf.db.table..[[
            ]]..where..[[
            LIMIT 1
        )
        ]]..andwhere..[[
        LIMIT 1
        ]]
    end

    return dbreq(sql)
end

function max(match)
    local key = ngx.req.get_uri_args()['key']
    if not key then ngx.exit(403) end
    -- Make sure valid request, only accept plain lowercase ascii string for key name
    local keytest = ngx.re.match(key, '[a-z]+', 'oj')
    if not keytest then ngx.exit(403) end

    local sql = [[
        SELECT
            date_trunc('day', timestamp) AS datetime,
            MAX(]]..key..[[) AS ]]..key..[[
            FROM ]]..conf.db.table..[[
            WHERE date_part('year', timestamp) < 2013
            GROUP BY 1
	]]

    return dbreq(sql)
end
