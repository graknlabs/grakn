/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.engine.controller;

import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.tasks.TaskId;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.multiqueue.MultiQueueTaskManager;
import ai.grakn.test.EngineContext;
import ai.grakn.test.engine.tasks.LongExecutionTestTask;
import ai.grakn.test.engine.tasks.ShortExecutionTestTask;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.put;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;

public class TasksControllerTest {
    private TaskId singleTask;

    @ClassRule
    public static final EngineContext engine = EngineContext.startMultiQueueServer();

    @BeforeClass
    public static void startEngine() throws Exception{
        ((Logger) org.slf4j.LoggerFactory.getLogger(TasksController.class)).setLevel(Level.DEBUG);
    }

    @Before
    public void setUp() throws Exception {
        MultiQueueTaskManager manager = (MultiQueueTaskManager) engine.getTaskManager();
        String creator = this.getClass().getName();
        TaskState status = TaskState.of(ShortExecutionTestTask.class, creator, TaskSchedule.now(), Json.object()).status(COMPLETED);
        singleTask = manager.storage().newState(status);
    }

    @Test
    public void testTasksByStatus() throws Exception{
        Response response = given().queryParam("status", COMPLETED.toString())
                                   .queryParam("limit", 10)
                                   .get("/tasks/all");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));

        JSONArray array = new JSONArray(response.body().asString());
        array.forEach(x -> {
            JSONObject o = (JSONObject)x;
            assertEquals(COMPLETED.toString(), o.get("status"));
        });
    }

    @Test
    public void testTasksByClassName() {
        Response response = given().queryParam("className", ShortExecutionTestTask.class.getName())
                                   .queryParam("limit", 10)
                                   .get("/tasks/all");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));

        JSONArray array = new JSONArray(response.body().asString());
        array.forEach(x -> {
            JSONObject o = (JSONObject)x;
            assertEquals(ShortExecutionTestTask.class.getName(), o.get("className"));
        });
    }

    @Test
    public void testTasksByCreator() {
        Response response = given().queryParam("creator", this.getClass().getName())
                                   .queryParam("limit", 10)
                                   .get("/tasks/all");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));

        JSONArray array = new JSONArray(response.body().asString());
        array.forEach(x -> {
            JSONObject o = (JSONObject)x;
            Assert.assertEquals(this.getClass().getName(), o.get("creator"));
        });
    }

    @Test
    public void testGetAllTasks() {
        Response response = given().queryParam("limit", 10).get("/tasks/all");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));
    }

    @Test
    public void testGetTask() throws Exception {
        get("/tasks/"+singleTask)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("id", equalTo(singleTask.getValue()));

        // Stopping tasks is not currently supported by the MultiQueueTaskManager.
//                .and().body("status", equalTo(STOPPED.toString()));
    }

    @Test
    public void testScheduleWithoutOptional() {
        given().queryParam("className", ShortExecutionTestTask.class.getName())
               .queryParam("creator", this.getClass().getName())
               .queryParam("runAt", new Date())
               .post(TASKS_SCHEDULE_URI)
               .then().statusCode(200)
               .and().contentType(ContentType.JSON)
               .and().body("id", notNullValue());
    }

    // Stopping tasks is not currently implemented in MultiQueueTaskManager
    @Ignore
    @Test
    public void testScheduleStopTask() {
        Response response = given()
                .queryParam("className", LongExecutionTestTask.class.getName())
                .queryParam("creator", this.getClass().getName())
                .queryParam("runAt", new Date())
                .queryParam("interval", 5000)
                .post(TASKS_SCHEDULE_URI);
        System.out.println(response.body().asString());

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON);

        String id = new JSONObject(response.body().asString()).getString("id");
        System.out.println(id);

        // Stop task
        put("/tasks/"+id+"/stop")
                .then().statusCode(200)
                .and().contentType("text/html");

        // Check state
        get("/tasks/"+id)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("id", equalTo(id))
                .and().body("status", equalTo(STOPPED.toString()));
    }
}
