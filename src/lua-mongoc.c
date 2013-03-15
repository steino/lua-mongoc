/*
* lua-mongoc.c: Binding for mongo-c-driver library.
*               See copyright information in file COPYRIGHT.
*/

#include <lua.h>
#include <lauxlib.h>
#include <math.h>
#include <string.h>
#include "mongo.h"

#define LUAMONGOC_VERSION 		"lua-mongoc 0.1"
#define LUAMONGOC_COPYRIGHT 		"Copyright (C) 2013, lua-mongoc authors"
#define LUAMONGOC_DESCRIPTION 		"Binding for the mongo C driver."

#define LUAMONGOC_CONN_MT 		"lua-mongoc.connection"
#define LUAMONGOC_CUR_MT 		"lua-mongoc.cursor"

typedef struct luamongoc_Connection
{
	mongo* obj;
} luamongoc_Connection;

typedef struct luamongoc_Cursor
{
	const char * key;
	int table;
	int count;
	mongo_cursor* obj;
} luamongoc_Cursor;

typedef struct luamongoc_bsontype
{
	bson * obj;
} luamongoc_bsontype;

// luaL_setfuncs() from lua 5.2
static void setfuncs (lua_State *L, const luaL_reg*l, int nup)
{
	luaL_checkstack(L, nup, "Too many upvalues.");
	for(; l && l->name; l++) {
		int i;
		for (i = 0; i < nup; i++)
			lua_pushvalue(L, -nup);
		lua_pushcclosure(L, l->func, nup);
		lua_setfield(L, -(nup + 2), l->name);
	}
	lua_pop(L, nup);
}

static void lua_append_bson(lua_State * L, const char * key, int idx, bson * b, int ref)
{
	bson * bobj = bson_create();
	bson_iterator * b_it = bson_iterator_create();

	double numval;
	int array = 1, len, intval;
	luamongoc_bsontype * bsontype = NULL;
	switch(lua_type(L, idx))
	{
		case 5:
			if (idx < 0)
				idx = lua_gettop(L) + idx + 1;

			lua_checkstack(L, 3);
			bson_init(bobj);

			lua_rawgeti(L, (-10000), ref);
			lua_pushvalue(L, idx);
			lua_pushboolean(L, 1);
			lua_rawset(L, -3);
			lua_pop(L, 1);

			lua_pushnil(L);
			while(lua_next(L, idx)) {
				++len;
				if ((lua_type(L, -2) != LUA_TNUMBER) || (lua_tointeger(L, -2) != len)) {
					lua_pop(L, 2);
					array = 0;
					break;
				}
				lua_pop(L, 1);
			}

			switch(array)
			{
				case 0:
					lua_pushnil(L);
					while(lua_next(L, idx)) {
						bson_append_start_object(bobj, key);
						switch (lua_type(L, -2)) {
							case 3: // number
							case 4: // string
								lua_append_bson(L, lua_tostring(L, -2), -1, bobj, ref);
								break;
						}
						bson_append_finish_object(bobj);
						lua_pop(L, 1);
					}
					break;
				case 1:
					bson_append_start_array(bobj, key);
					for (int i = 0; i < len; i++) {
						lua_pushinteger(L, i);
						lua_rawgeti(L, idx, i+1);
						lua_append_bson(L, lua_tostring(L, -2), -1, bobj, ref);
						lua_pop(L, 2);
					}
					bson_append_finish_array(bobj);
					break;
			}
			if(bson_finish(bobj) != 0)
			{
				printf("BSON ERROR");
				return;
			}
			bson_iterator_init(b_it, bobj);
			bson_find(b_it, bobj, key);
			bson_append_element(b, key, b_it);
			bson_iterator_dispose(b_it);
			bson_destroy(bobj);
			break;
		case 3: // number
			numval = lua_tonumber(L, idx);
			if(numval == floor(numval))
			{
				intval = lua_tointeger(L, idx);
				bson_append_int(b, key, intval);
			} else {
				bson_append_double(b, key, numval);
			}
			break;
		case 4: // string
			bson_append_string(b, key, lua_tostring(L, idx));
			break;
		case 1: // bool
			bson_append_bool(b, key, lua_toboolean(L, idx));
			break;
		case 0: // nil
			bson_append_null(b, key);
			break;
		case 7: // userdata
			bsontype = (luamongoc_bsontype *)lua_touserdata(L, idx);
			bson_iterator_init(b_it, bsontype->obj);
			bson_find(b_it, bsontype->obj, "bsontype");
			bson_append_element(b, key, b_it);
			bson_iterator_dispose(b_it);
			break;
	}
}

static void bson_to_value(lua_State * L, bson_iterator * it);

static void bson_to_array (lua_State * L, bson *b)
{
	bson_iterator * it = bson_iterator_create();
	bson_iterator_init(it, b);

	lua_newtable(L);
	int n = 1,i = bson_size(b);
	for(i; i--;)
	{
		bson_type type;
		type = bson_iterator_next(it);

		if(type == 0)
			break;

		bson_to_value(L, it);
		lua_rawseti(L, -2, n++);
	}

	bson_iterator_dispose(it);
}

static void bson_to_table (lua_State * L, bson *b)
{
	bson_iterator * it = bson_iterator_create();
	bson_iterator_init(it, b);

	lua_newtable(L);
	int i = bson_size(b);
	for(i; i--;)
	{
		bson_type type;
		type = bson_iterator_next(it);

		if(type == 0)
			break;

		const char * key = bson_iterator_key(it);

		lua_pushstring(L, key);
		bson_to_value(L, it);
		lua_rawset(L, -3);
	}

	bson_iterator_dispose(it);
}

static void bson_to_value(lua_State * L, bson_iterator * it)
{
	bson * sub = bson_create();
	bson_type type;
	type = bson_iterator_type(it);
	char oid[25];

	lua_checkstack(L, 2);
	switch(type)
	{
		case 18: // long
		case 1: // double
			lua_pushnumber(L, bson_iterator_double(it));
			break;
		case 16: // int
			lua_pushinteger(L, bson_iterator_int(it));
			break;
		case 2: // string
			lua_pushstring(L, bson_iterator_string(it));
			break;
		case 4: // array
			bson_iterator_subobject(it, sub);
			bson_to_array(L, sub);
			break;
		case 3: // object
			bson_iterator_subobject(it, sub);
			bson_to_table(L, sub);
			break;
		case 7: // oid
			bson_oid_to_string(bson_iterator_oid(it), oid);
			lua_pushstring(L, oid);
			break;
	}
}

void lua_to_bson (lua_State * L, int idx, bson * b)
{
	lua_newtable(L);
	int ref = luaL_ref(L, (-10000));

	lua_pushnil(L);
	while(lua_next(L, idx))
	{
		switch (lua_type(L, -2))
		{
			case 3: // number
			case 4: // string
				lua_append_bson(L, lua_tostring(L, -2), -1, b, ref);
				break;
		}
		lua_pop(L, 1);
	}

	luaL_unref(L, (-10000), ref);
}

static mongo * check_connection(lua_State * L, int idx)
{
	luamongoc_Connection * conn = (luamongoc_Connection *)luaL_checkudata(L, idx, LUAMONGOC_CONN_MT);

	if (conn == NULL)
	{
		luaL_error(L, "lua-mongoc error: connection is null");
		return NULL;
	}
	if (conn->obj == NULL)
	{
		luaL_error(L, "lua-mongoc error: attempted to use closed connection");
		return NULL;
	}

	return conn->obj;
}

static int lcur_iterator(lua_State * L)
{
	luamongoc_Cursor * cursor_mt = (luamongoc_Cursor *)luaL_checkudata(L, lua_upvalueindex(1), LUAMONGOC_CUR_MT);
	mongo_cursor * cursor = cursor_mt->obj;

	if(cursor->seen > cursor_mt->count) {
		mongo_cursor_next(cursor);
		bson * b = bson_create();
		bson_copy(b, mongo_cursor_bson(cursor));
		if(cursor_mt->table) {
			bson_to_table(L, b);
		} else {
			bson_iterator * it = bson_iterator_create();
			bson_iterator_init(it, b);
			bson_find(it, b, cursor_mt->key);
			bson_to_value(L, it);
			bson_iterator_dispose(it);
		}
		cursor_mt->count++;
	} else {
		lua_pushnil(L);
	}
	return 1;
}

static int lcur_results(lua_State * L)
{
	lua_pushcclosure(L, lcur_iterator, 1);
	return 1;
}

static int lcur_count(lua_State * L)
{
	luamongoc_Cursor * cursor_mt = (luamongoc_Cursor *)luaL_checkudata(L, 1, LUAMONGOC_CUR_MT);
	mongo_cursor * cursor = cursor_mt->obj;

	lua_pushinteger(L, cursor->seen);
	return 1;
}

static int lcur_next(lua_State * L)
{
	luamongoc_Cursor * cursor_mt = (luamongoc_Cursor *)luaL_checkudata(L, 1, LUAMONGOC_CUR_MT);
	mongo_cursor * cursor = cursor_mt->obj;

	if(cursor->seen > cursor_mt->count) {
		mongo_cursor_next(cursor);
		bson * b = bson_create();
		bson_copy(b, mongo_cursor_bson(cursor));
		if(cursor_mt->table) {
			bson_to_table(L, b);
		} else {
			bson_iterator * it = bson_iterator_create();
			bson_iterator_init(it, b);
			bson_find(it, b, cursor_mt->key);
			bson_to_value(L, it);
			bson_iterator_dispose(it);
		}
		cursor_mt->count++;
	} else {
		lua_pushnil(L);
	}

	return 1;
}

static int lcur_more(lua_State * L)
{
	luamongoc_Cursor * cursor_mt = (luamongoc_Cursor *)luaL_checkudata(L, 1, LUAMONGOC_CUR_MT);
	mongo_cursor * cursor = cursor_mt->obj;

	if(cursor->seen > cursor_mt->count) {
		lua_pushboolean(L, 1);
	} else {
		lua_pushboolean(L, 0);
	}

	return 1;
}

static int lcur_tostring(lua_State * L)
{
	luaL_checkstack(L, 1, "not enough stack to push reply");
	lua_pushliteral(L, LUAMONGOC_CUR_MT);

	return 1;
}


static const struct luaL_reg C[] =
{
	{ "results", lcur_results },
	{ "count", lcur_count },
	{ "next", lcur_next },
	{ "more", lcur_more },

	{ NULL, NULL }
};


static int lconn_find(lua_State *L)
{
	luamongoc_Cursor * result = NULL;

	bson * q = bson_create();
	bson * f = bson_create();
	bson_init(q);
	bson_init(f);

	const char * key = NULL;
	int limit, skip = 0, table = 1;

	mongo * conn = check_connection(L, 1);
	const char * ns = luaL_checkstring(L, 2);
	luaL_checktype(L, 3, 5);
	lua_to_bson(L, 3, q);
	if (bson_finish(q) != 0)
	{
		printf("BSON ERROR");
		return 0;
	}

	switch(lua_type(L, 4))
	{
		case 5:
			lua_to_bson(L, 4, f);
			break;
		case 4:
			key = lua_tostring(L, 4);
			bson_append_int(f, key, 1);
			table = 0;
			break;
	}

	if(bson_finish(f) != 0)
	{
		printf("BSON ERROR");
		return 0;
	}

	if(!lua_isnoneornil(L, 5)) {
		luaL_checkinteger(L, 5);
		limit = lua_tonumber(L, 5);
	}

	if(!lua_isnoneornil(L, 6)) {
		luaL_checkinteger(L, 6);
		skip = lua_tonumber(L, 5);
	}

	mongo_cursor * cursor = mongo_find(conn, ns, q, f, limit, skip, 0);

	bson_destroy(q);
	bson_destroy(f);

	if(cursor == NULL) {
		luaL_checkstack(L, 2, "Not enough stack to push error");
		lua_pushnil(L);
		lua_pushinteger(L, conn->err);
		return 2;
	}

	luaL_checkstack(L, 1, "Not enough stack to create connection");
	result = (luamongoc_Cursor *)lua_newuserdata(
			L, sizeof(luamongoc_Cursor)
			);
	result->obj = cursor;
	result->count = 0;
	result->key = key;
	result->table = table;

	if (luaL_newmetatable(L, LUAMONGOC_CUR_MT))
	{
		luaL_checkstack(L, 1, "Not enough stack to register connection MT");
		lua_pushvalue(L, lua_upvalueindex(1));
		setfuncs(L, C, 1);

		lua_pushvalue(L, -1);
		lua_setfield(L, -2, "__index");
	}
	lua_setmetatable(L, -2);

	return 1;
}

#define lconn_query lconn_find

static int lconn_find_one(lua_State *L)
{
	bson * q = bson_create();
	bson * f = bson_create();
	bson * out = bson_create();
	bson_init(q);
	bson_init(f);
	bson_init(out);

	const char * key = NULL;
	int table = 1;

	mongo * conn = check_connection(L, 1);
	const char * ns = luaL_checkstring(L, 2);
	luaL_checktype(L, 3, 5);
	lua_to_bson(L, 3, q);
	if (bson_finish(q) != 0)
	{
		printf("BSON ERROR");
		return 0;
	}
	switch(lua_type(L, 4))
	{
		case 5:
			lua_to_bson(L, 4, f);
			break;
		case 4:
			key = lua_tostring(L, 4);
			bson_append_int(f, key, 1);
			table = 0;
			break;
	}

	if(bson_finish(f) != 0)
	{
		printf("BSON ERROR");
		return 0;
	}

	if(mongo_find_one(conn, ns, q, f, out) != 0)
	{
		bson_destroy(q);
		bson_destroy(f);
		luaL_checkstack(L, 2, "Not enough stack to push error");
		lua_pushnil(L);
		lua_pushstring(L, conn->lasterrstr);
		return 2;
	}

	bson_destroy(q);
	bson_destroy(f);

	if(table) {
		bson_to_table(L, out);
	} else {
		bson_iterator * it = bson_iterator_create();
		bson_iterator_init(it, out);
		bson_find(it, out, key);
		bson_to_value(L, it);
	}
	return 1;
}

static int lconn_drop_collection(lua_State *L)
{
	bson * b = bson_create();
	mongo * conn = check_connection(L, 1);

	const char * s = luaL_checkstring(L, 2);
	char * e = strchr(s, *".");

	lua_pushlstring(L, s, e-s);

	const char * db = lua_tostring(L, -1);
	const char * coll = e+1;

	lua_pop(L, 1);

	if(mongo_cmd_drop_collection(conn, db, coll, b) != 0)
	{
		luaL_checkstack(L, 2, "Not enough stack to push error");
		lua_pushnil(L);
		return 1;
	}

	lua_pushboolean(L, 1);
	return 1;
}

static int lconn_count(lua_State *L)
{
	bson * b = bson_create();
	bson_init(b);
	mongo * conn = check_connection(L, 1);

	const char * s = luaL_checkstring(L, 2);
	char * e = strchr(s, *".");

	lua_pushlstring(L, s, e-s);

	const char * db = lua_tostring(L, -1);
	const char * coll = e+1;

	lua_pop(L, 1);

	if(!lua_isnoneornil(L, 3)) {
		luaL_checktype(L, 3, 5);
		lua_to_bson(L, 3, b);
	}

	if(bson_finish(b) != 0)
	{
		printf("BSON ERROR");
		return 0;
	}

	int count = mongo_count(conn, db, coll, b);
	if(count == -1) {
		bson_destroy(b);
		luaL_checkstack(L, 2, "Not enough stack to push error");
		lua_pushnil(L);
		lua_pushstring(L, conn->lasterrstr);
		return 2;
	}
	bson_destroy(b);
	lua_pushinteger(L, count);
	return 1;
}

static int lconn_update(lua_State *L)
{
	bson * cond = bson_create();
	bson * op = bson_create();
	bson_init(cond);
	bson_init(op);

	mongo * conn = check_connection(L, 1);
	const char * ns = luaL_checkstring(L, 2);
	luaL_checktype(L, 3, 5);
	luaL_checktype(L, 4, 5);

	lua_to_bson(L, 3, cond);
	lua_to_bson(L, 4, op);

	if(bson_finish(cond) != 0 || bson_finish(op) != 0)
	{
		printf("BSON ERROR");
		return 0;
	}

	int upsert = lua_toboolean(L, 5);
	int multi = lua_toboolean(L, 6);
	int flags = 0x4;

	if(upsert)
		flags = flags | 0x1; // upsert
	if(multi)
		flags = flags | 0x2; // multi

	if(mongo_update(conn, ns, cond, op, flags, NULL) != 0)
	{
		bson_destroy(cond);
		bson_destroy(op);
		luaL_checkstack(L, 2, "Not enough stack to push error");
		lua_pushnil(L);
		lua_pushstring(L, conn->lasterrstr);
		return 2;
	}

	bson_destroy(cond);
	bson_destroy(op);
	lua_pushboolean(L, 1);
	return 1;
}

static int lconn_insert(lua_State *L)
{
	bson * b = bson_create();
	bson_init(b);
	mongo * conn = check_connection(L, 1);
	const char * ns = luaL_checkstring(L, 2);

	luaL_checktype(L, 3, 5);

	lua_to_bson(L, 3, b);

	if(bson_finish(b) != 0)
	{
		printf("BSON ERROR");
		return 0;
	}
	if(mongo_insert(conn, ns, b, NULL) != 0)
	{
		bson_destroy(b);
		luaL_checkstack(L, 2, "Not enough stack to push error");
		lua_pushnil(L);
		lua_pushinteger(L, conn->err);
		return 2;
	}
	bson_destroy(b);
	lua_pushboolean(L, 1);
	return 1;
}

static int lconn_remove(lua_State *L)
{
	bson * b = bson_create();
	bson_init(b);
	mongo * conn = check_connection(L, 1);
	const char * ns = luaL_checkstring(L, 2);

	luaL_checktype(L, 3, 5);

	lua_to_bson(L, 3, b);
	if(bson_finish(b) != 0)
	{
		printf("BSON ERROR");
		return 0;
	}
	if(mongo_remove(conn, ns, b, NULL) != 0)
	{
		bson_destroy(b);
		luaL_checkstack(L, 2, "Not enough stack to push error");
		lua_pushnil(L);
		lua_pushinteger(L, conn->err);
		return 2;
	}
	bson_destroy(b);
	lua_pushboolean(L, 1);
	return 1;
}

static int lconn_close(lua_State * L)
{
	luamongoc_Connection * conn = (luamongoc_Connection *)luaL_checkudata(L, 1, LUAMONGOC_CONN_MT);
	if (conn && conn->obj != NULL)
	{
		mongo_destroy(conn->obj);
		conn->obj = NULL;
	}
	return 0;
}

#define lconn_gc lconn_close

static int lconn_tostring(lua_State * L)
{
	luaL_checkstack(L, 1, "not enough stack to push reply");
	lua_pushliteral(L, LUAMONGOC_CONN_MT);

	return 1;
}

static const luaL_reg M[] =
{
	{ "find", lconn_find },
	{ "query", lconn_query },
	{ "find_one", lconn_find_one },
	{ "drop_collection", lconn_drop_collection },
	{ "count", lconn_count },
	{ "update", lconn_update },
	{ "insert", lconn_insert },
	{ "remove", lconn_remove },

	{ "close", lconn_close },
	{ "__gc", lconn_gc },
	{ "__tostring", lconn_tostring },

	{ NULL, NULL }
};

static int lmongoc_bsontype_tostring(lua_State * L)
{
	luaL_getmetafield(L, 1, "__string");
	lua_pushstring(L, lua_tostring(L, -1));
	return 1;
}

void lmongoc_bsontype(lua_State * L, int type)
{
	luamongoc_bsontype * result = NULL;
	bson * b = bson_create();
	char string[25];

	bson_init(b);
	switch(type) {
		case 11:
			bson_append_regex(b, "bsontype", luaL_checkstring(L, 1), luaL_checkstring(L, 2));
			sprintf(string, "mongoc.RegEx: /%s/%s", lua_tostring(L, 1), lua_tostring(L, 2));
		break;
	}

	if(bson_finish(b) != 0)
	{
		printf("BSON ERROR");
		return;
	}

	luaL_checkstack(L, 1, "Not enough stack to create connection");
	result = (luamongoc_bsontype *)lua_newuserdata(L, sizeof(luamongoc_bsontype));
	result->obj = b;

	lua_newtable(L);

	lua_pushstring(L, string);
	lua_setfield(L, -2, "__string");

	lua_pushcfunction(L, lmongoc_bsontype_tostring);
	lua_setfield(L, -2, "__tostring");

	lua_setmetatable(L, -2);
}

static int lmongoc_regex(lua_State *L)
{
	lmongoc_bsontype(L, 11);
	return 1;
}

static int lmongoc_connect(lua_State * L)
{
	const char * host = luaL_checkstring(L, 1);
	int port = luaL_checkint(L, 2);

	luamongoc_Connection * result = NULL;
	mongo * conn = mongo_create();

	if(mongo_client(conn, host, port) != 0) {
		luaL_checkstack(L, 2, "Not enough stack to push error");
		lua_pushnil(L);
		lua_pushinteger(L, conn->err);
		return 2;
	}
	
	luaL_checkstack(L, 1, "Not enough stack to create connection");
	result = (luamongoc_Connection *)lua_newuserdata(
			L, sizeof(luamongoc_Connection)
			);
	result->obj = conn;

	if (luaL_newmetatable(L, LUAMONGOC_CONN_MT))
	{
		luaL_checkstack(L, 1, "Not enough stack to register connection MT");
		lua_pushvalue(L, lua_upvalueindex(1));
		setfuncs(L, M, 1);

		lua_pushvalue(L, -1);
		lua_setfield(L, -2, "__index");
	}
	lua_setmetatable(L, -2);
	
	return 1;
}

static const struct luaL_reg E[] =
{
	{ NULL, NULL }
};

static const struct luaL_reg R[] =
{
	{ "regex", lmongoc_regex },
	{ "connect", lmongoc_connect },

	{ NULL, NULL }
};

LUALIB_API int luaopen_mongoc(lua_State * L)
{
	luaL_register(L, "mongoc", E);

	lua_pushliteral(L, LUAMONGOC_VERSION);
	lua_setfield(L, -2, "_VERSION");

	lua_pushliteral(L, LUAMONGOC_COPYRIGHT);
	lua_setfield(L, -2, "_COPYRIGHT");

	lua_pushliteral(L, LUAMONGOC_DESCRIPTION);
	lua_setfield(L, -2, "_DESCRIPTION");

	lua_pushvalue(L, -1);
	setfuncs(L, R, 1);

	return 1;
}
