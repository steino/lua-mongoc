/*
* lua-mongoc.c: Binding for mongo-c-driver library.
*               See copyright information in file COPYRIGHT.
*/

#include <lua.h>
#include <lauxlib.h>

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

typedef struct luamongoc_Connection
{
	mongo* obj;
} luamongoc_Connection;

static mongo * check_connection(lua_State * L, int idx)
{
	luamongoc_Connection * conn = (luamongoc_Connection *)luaL_checkudata(
			L, idx, LUAMONGOC_CONN_MT
			);

	if (!conn->obj->connected)
		return 0;

	return conn->obj;
}


static int lconn_close(lua_State * L)
{
	luamongoc_Connection * conn = (luamongoc_Connection *)luaL_checkudata(
		L, 1, LUAMONGOC_CONN_MT
	);

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
