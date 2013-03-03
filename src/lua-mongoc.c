/*
* lua-mongoc.c: Binding for mongo-c-driver library.
*               See copyright information in file COPYRIGHT.
*/

#include <lua.h>
#include <lauxlib.h>
#include <math.h>

#include "mongo.h"

#define LUAMONGOC_VERSION 		"lua-mongoc 0.1"
#define LUAMONGOC_COPYRIGHT 	"Copyright (C) 2013, lua-mongoc authors"
#define LUAMONGOC_DESCRIPTION "Binding for the mongo C driver."

#define LUAMONGOC_CONN_MT 		"lua-mongoc.connection"

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
	int intval;
	int len;
	int array = 1;
	char newkey[15];
	int newkeyint;

	switch(lua_type(L, idx))
	{
		case LUA_TTABLE:
			if (idx < 0)
				idx = lua_gettop(L) + idx + 1;

			lua_checkstack(L, 3);
			bson_init(bobj);

			lua_rawgeti(L, LUA_REGISTRYINDEX, ref);
			lua_pushvalue(L, idx);
			lua_pushboolean(L, 1);
			lua_rawset(L, -3);
			lua_pop(L, 1);

			for (lua_pushnil(L); lua_next(L, idx); lua_pop(L, 1)) {
				++len;
				if ((lua_type(L, -2) != LUA_TNUMBER) || (lua_tointeger(L, -2) != len)) {
					lua_pop(L, 2);
					array = 0;
					break;
				}
			}

			if(array)
			{
				bson_append_start_array(bobj, key);
				for (int i = 0; i < len; i++) {
					lua_rawgeti(L, idx, i+1);
					sprintf(newkey, "%d", i);
					lua_append_bson(L, newkey, -1, bobj, ref);
					lua_pop(L, 1);
				}
				bson_append_finish_array(bobj);

				if(bson_finish(bobj) != BSON_OK)
				{
					printf("BSON ERROR");
					return;
				}

				bson_iterator_init(b_it, bobj);
				bson_find(b_it, bobj, key);
				bson_append_element(b, key, b_it);
				bson_iterator_dispose(b_it);
				bson_destroy(bobj);
			} else {
				for (lua_pushnil(L); lua_next(L, idx); lua_pop(L, 1)) {
					switch (lua_type(L, -2)) {
						case LUA_TNUMBER:
							newkeyint = lua_tonumber(L, -2);
							sprintf(newkey, "%d", newkeyint);
							lua_append_bson(L, newkey, -1, bobj, ref);
							break;
						case LUA_TSTRING:
							lua_append_bson(L, lua_tostring(L, -2), -1, bobj, ref);
							break;
					}
				}
				if(bson_finish(bobj) != BSON_OK)
				{
					printf("BSON ERROR");
					return;
				}
				bson_append_bson(b, key, bobj);
				bson_destroy(bobj);
			}
			break;
		case LUA_TNUMBER:
			numval = lua_tonumber(L, idx);
			if(numval == floor(numval))
			{
				intval = lua_tointeger(L, idx);
				bson_append_int(b, key, intval);
			} else {
				bson_append_double(b, key, numval);
			}
			break;
		case LUA_TSTRING:
			bson_append_string(b, key, lua_tostring(L, idx));
			break;
		case LUA_TBOOLEAN:
			bson_append_bool(b, key, lua_toboolean(L, idx));
			break;
		case LUA_TNIL:
			bson_append_null(b, key);
			break;
	}
}

static void lua_to_bson (lua_State * L, int idx, bson * b)
{
	lua_newtable(L);
	int ref = luaL_ref(L, LUA_REGISTRYINDEX);

	int keyint;
	char key[15];

	for (lua_pushnil(L); lua_next(L, idx); lua_pop(L, 1))
	{
		switch (lua_type(L, -2))
		{
			case LUA_TNUMBER:
				keyint = lua_tointeger(L, -2);
				sprintf(key, "%d", keyint);
				lua_append_bson(L, key, -1, b, ref);
				break;
			case LUA_TSTRING:
				lua_append_bson(L, lua_tostring(L, -2), -1, b, ref);
				break;
		}
	}

	luaL_unref(L, LUA_REGISTRYINDEX, ref);
}

typedef struct luamongoc_Connection
{
	mongo* obj;
} luamongoc_Connection;

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
	}

	return conn->obj;
}

static int lconn_count(lua_State *L)
{
	bson * b = bson_create();
	bson_init(b);
	mongo * conn = check_connection(L, 1);
	const char * db = luaL_checkstring(L, 2);
	const char * coll = luaL_checkstring(L, 3);

	if(!lua_isnoneornil(L, 4) && !lua_type(L, 4) == LUA_TTABLE)
	{
		luaL_checkstack(L, 2, "Not enough stack to push error");
		lua_pushnil(L);
		lua_pushliteral(L, "Query is not a table");
	} else if (lua_type(L, 4) == LUA_TTABLE) {
		lua_to_bson(L, 4, b);

		if(bson_finish(b) != BSON_OK)
		{
			printf("BSON ERROR");
			return 0;
		}
	}
	int count = mongo_count(conn, db, coll, b);
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
	luaL_checktype(L, 3, LUA_TTABLE);
	luaL_checktype(L, 4, LUA_TTABLE);

	lua_to_bson(L, 3, cond);
	lua_to_bson(L, 4, op);

	if(bson_finish(cond) != BSON_OK || bson_finish(op) != BSON_OK)
	{
		printf("BSON ERROR");
		return 0;
	}

	int upsert = lua_toboolean(L, 5);
	int multi = lua_toboolean(L, 6);
	int flags = MONGO_UPDATE_BASIC;

	if(upsert)
		flags = flags | MONGO_UPDATE_UPSERT;
	if(multi)
		flags = flags | MONGO_UPDATE_MULTI;

	if(mongo_update(conn, ns, cond, op, flags, NULL) != MONGO_OK)
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

	luaL_checktype(L, 3, LUA_TTABLE);

	lua_to_bson(L, 3, b);

	if(bson_finish(b) != BSON_OK)
	{
		printf("BSON ERROR");
		return 0;
	}
	if(mongo_insert(conn, ns, b, NULL) != MONGO_OK)
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
	{ "count", lconn_count },
	{ "update", lconn_update },
	{ "insert", lconn_insert },

	{ "close", lconn_close },
	{ "__gc", lconn_gc },
	{ "__tostring", lconn_tostring },

	{ NULL, NULL }
};

static int lmongoc_connect(lua_State * L)
{
	const char * host = luaL_checkstring(L, 1);
	int port = luaL_checkint(L, 2);

	luamongoc_Connection * result = NULL;
	mongo * conn = mongo_create();

	int status = mongo_client(conn, host, port);
	if( status != MONGO_OK ) {
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
