local mongoc = assert(require'mongoc')

local host = "127.0.0.1"
local port = 27017

assert(mongoc.connect(host, 1) == nil)

local conn = assert(mongoc.connect(host, port))

assert(conn:insert("test.mongoc", { mykey = "myvalue" }))

assert(conn:count("test.mongoc") == 1)

assert(assert(conn:find_one("test.mongoc", { mykey = "myvalue" }, "mykey")) == "myvalue")
local query = assert(conn:find("test.mongoc", { mykey = "myvalue" }))

assert(query:count() == 1)

for res in query:results() do
	assert(type(res) == "table")
	assert(res["mykey"] == "myvalue")
end

assert(conn:count("test.mongoc") == 1)

assert(conn:remove("test.mongoc", { mykey = "myvalue" }))

-- Array
assert(conn:insert("test.mongoc", { type = "array", myarray = { "one", "two", "three", "four", "five" }}))
local query = assert(conn:find_one("test.mongoc", { type = "array"}))

assert(type(query["myarray"]) == "table")

assert(query.myarray[1] == "one")
assert(query.myarray[2] == "two")
assert(query.myarray[3] == "three")
assert(query.myarray[4] == "four")
assert(query.myarray[5] == "five")

local query = assert(conn:find_one("test.mongoc", { type = "array" }, "myarray"))

assert(type(query) == "table")

assert(query[1] == "one")
assert(query[2] == "two")
assert(query[3] == "three")
assert(query[4] == "four")
assert(query[5] == "five")

assert(conn:remove("test.mongoc", { type = "array" }))

-- Object
assert(conn:insert("test.mongoc", { type = "object", myobject = { mykey = "myvalue" } }))
local query = assert(conn:find_one("test.mongoc", { type = "object"}))

assert(type(query.myobject) == "table")
assert(query.myobject.mykey == "myvalue")

local query = assert(conn:find_one("test.mongoc", { type = "object" }, "myobject"))
assert(type(query) == "table")
assert(query.mykey == "myvalue")

assert(conn:remove("test.mongoc", { type = "object" }))

assert(conn:drop_collection("test.mongoc"))

print("Tests passed!")
