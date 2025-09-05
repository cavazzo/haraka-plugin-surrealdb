"use strict";

const { Surreal } = require("surrealdb");

exports.register = function () {
    this.load_surrealdb_ini();

    // another plugin has called us with: inherits('haraka-plugin-surrealdb')
    if (this.name !== "surrealdb") return;

    // register when 'surrealdb' is declared in config/plugins
    this.register_hook("init_master", "init_surrealdb_shared");
    this.register_hook("init_child", "init_surrealdb_shared");
};

exports.load_surrealdb_ini = function () {
    const plugin = this;

    // store surrealdb cfg at surrealdbCfg, to avoid conflicting with plugins that
    // inherit this plugin and have *their* config at plugin.cfg
    plugin.surrealdbCfg = plugin.config.get("surrealdb.ini", () => {
        plugin.load_surrealdb_ini();
    });
};

exports.merge_surrealdb_ini = function () {
    if (!this.cfg) this.cfg = {}; // no <plugin>.ini loaded?
    if (!this.cfg.surrealdb) this.cfg.surrealdb = {}; // no [surrealdb] in <plugin>.ini file
    if (!this.surrealdbCfg) this.load_redis_ini();

    this.cfg.surrealdb = Object.assign(
        {},
        this.surrealdbCfg.server,
        this.cfg.surrealdb
    );
};

exports.init_surrealdb_shared = async function (next, server) {
    let calledNext = false;
    function nextOnce(e) {
        if (e) this.logerror(`Surrealdb error: ${e.message}`);
        if (calledNext) return;
        calledNext = true;
        next();
    }

    if (!server.notes.surrealdb) {
        server.notes.surrealdb = await this.get_surrealdb_client(
            this.surrealdbCfg.server
        );
        nextOnce();
        return;
    }

    const connected = await server.notes.surrealdb.ping();

    if (!connected) {
        return nextOnce(err);
    } else {
        this.loginfo("already connected");
        nextOnce(); // connection is good
    }
};

exports.get_surrealdb_client = async function (opts) {
    const client = new Surreal();

    try {
        const getConnection = async () => {
            return client.connect(`http://${opts.host}:${opts.port}/rpc`, {
                namespace: opts.namespace,
                database: opts.database,
                auth: {
                    username: opts.username,
                    password: opts.password
                }
            });
        };

        const closeConnection = async (client) => {
            await client.invalidate();
            await client.close();
        };

        this.loginfo(`SurrealDB connected to http://${opts.host}:${opts.port}/rpc`);

        const getAll = async (table) => {
            const connection = await getConnection();
            const records = await connection.select(table);
            await closeConnection(connection);
            return records;
        };

        const ping = async () => {
            try {
                const connection = await getConnection();
                await connection.info();
                await closeConnection(connection);
                return true;
            } catch {
                return false;
            }
        };

        return {
            getAll,
            ping
        };
    } catch (error) {
        this.logerror(error);
    }
};
