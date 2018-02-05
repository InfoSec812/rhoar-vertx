package com.redhat.labs.adjective.services;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

public class AdjectiveServiceImpl implements AdjectiveService {

    SQLClient client;

    /**
     * Default Constructor
     * @param vertx The {@link Vertx} instance for the running application
     */
    public AdjectiveServiceImpl(Vertx vertx) {
        JsonObject dbConfig = vertx.getOrCreateContext().config().getJsonObject("db");
        client = JDBCClient.createShared(vertx, dbConfig, "adjective");
    }

    @Override
    public void save(String adjective, Handler<AsyncResult<String>> resultHandler) {
        client.getConnection(connRes -> saveConnHandler(adjective, resultHandler, connRes));
    }

    /**
     * Handle the result of requesting a DB Connection
     * @param adjective A {@link String} representing the adjective to be added to the DB
     * @param resultHandler A {@link Handler} to be used as a callback once the request is complete
     * @param connRes An {@link AsyncResult} which MAY contain a {@link SQLConnection} if successful
     */
    private void saveConnHandler(String adjective, Handler<AsyncResult<String>> resultHandler, AsyncResult<SQLConnection> connRes) {
        if (connRes.succeeded()) {
            SQLConnection conn = connRes.result();
            JsonArray params = new JsonArray().add(adjective);
            conn.queryWithParams(
                    "INSERT INTO adjectives (adjective) VALUES (?)",
                    params,
                    queryRes -> handleSaveResult(adjective, resultHandler, queryRes));
        } else {
            resultHandler.handle(Future.failedFuture(connRes.cause()));
        }
    }

    /**
     * Handle the result of a save query execution
     * @param adjective A {@link String} representing the adjective to be added to the DB
     * @param resultHandler A {@link Handler} to be used as a callback once the request is complete
     * @param queryRes An {@link AsyncResult} which MAY contain a {@link ResultSet} if successful
     */
    private void handleSaveResult(String adjective, Handler<AsyncResult<String>> resultHandler, AsyncResult<ResultSet> queryRes) {
        if (queryRes.succeeded()) {
            JsonObject result = new JsonObject()
                    .put("url", String.format("/rest/v1/adjective/%s", adjective));
            resultHandler.handle(Future.succeededFuture(result.encodePrettily()));
        } else {
            resultHandler.handle(Future.failedFuture(queryRes.cause()));
        }
    }

    @Override
    public void get(Handler<AsyncResult<String>> resultHandler) {
        client.getConnection(connRes -> handleGetConnectionResult(resultHandler, connRes));
    }

    /**
     *
     * @param resultHandler A {@link Handler} to be used as a callback once the request is complete
     * @param connRes An {@link AsyncResult} which MAY contain a {@link SQLConnection} if successful
     */
    private void handleGetConnectionResult(Handler<AsyncResult<String>> resultHandler, AsyncResult<SQLConnection> connRes) {
        if (connRes.succeeded()) {
            SQLConnection conn = connRes.result();
            conn.query(
                    "SELECT adjective FROM adjectives ORDER BY RAND() LIMIT 1",
                    queryRes -> handleGetQueryResult(resultHandler, conn, queryRes));
        } else {
            resultHandler.handle(Future.failedFuture(connRes.cause()));
        }
    }

    /**
     *
     * @param resultHandler A {@link Handler} to be used as a callback once the request is complete
     * @param conn A {@link SQLConnection} can be used to execute queries
     * @param queryRes An {@link AsyncResult} which MAY contain a {@link ResultSet} if successful
     */
    private void handleGetQueryResult(Handler<AsyncResult<String>> resultHandler, SQLConnection conn, AsyncResult<ResultSet> queryRes) {
        if (queryRes.succeeded()) {
            System.out.println("Got adjective from DB");
            ResultSet resultSet = queryRes.result();
            JsonObject result = resultSet.getRows().get(0);
            resultHandler.handle(Future.succeededFuture(result.encodePrettily()));
            conn.close();
        } else {
            System.out.println("Failed to get adjective from DB");
            resultHandler.handle(Future.failedFuture(queryRes.cause()));
        }
    }

    /**
     *
     * @param resultHandler A {@link Handler} to be used as a callback once the request is complete
     */
    public void check(Handler<AsyncResult<String>> resultHandler) {
        client.getConnection(connRes -> handleStatusConnectionRequest(resultHandler, connRes));
    }

    /**
     *
     * @param resultHandler A {@link Handler} to be used as a callback once the request is complete
     * @param connRes An {@link AsyncResult} which MAY contain a {@link SQLConnection} if successful
     */
    private void handleStatusConnectionRequest(Handler<AsyncResult<String>> resultHandler, AsyncResult<SQLConnection> connRes) {
        if (connRes.succeeded()) {
            SQLConnection conn = connRes.result();
            conn.query(
                    "SELECT 1 FROM adjectives LIMIT 1",
                    queryRes -> handleStatusQueryResult(resultHandler, conn, queryRes));
        } else {
            resultHandler.handle(Future.failedFuture(connRes.cause()));
        }
    }

    /**
     *
     * @param resultHandler A {@link Handler} to be used as a callback once the request is complete
     * @param conn A {@link SQLConnection} can be used to execute queries
     * @param queryRes An {@link AsyncResult} which MAY contain a {@link ResultSet} if successful
     */
    private void handleStatusQueryResult(Handler<AsyncResult<String>> resultHandler, SQLConnection conn, AsyncResult<ResultSet> queryRes) {
        if (queryRes.succeeded()) {
            resultHandler.handle(Future.succeededFuture(new JsonObject().put("status", "OK").encodePrettily()));
            conn.close();
        } else {
            resultHandler.handle(Future.failedFuture(queryRes.cause()));
        }
    }
}
