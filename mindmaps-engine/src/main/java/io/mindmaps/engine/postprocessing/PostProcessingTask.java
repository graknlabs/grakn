/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.postprocessing;

import io.mindmaps.engine.backgroundtasks.BackgroundTask;
import io.mindmaps.engine.loader.RESTLoader;
import io.mindmaps.engine.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PostProcessingTask implements BackgroundTask {
    private final Logger LOG = LoggerFactory.getLogger(PostProcessingTask.class);

    private static PostProcessing instance = null;
    private static long timeLapse;

    private PostProcessing postProcessing;

    public PostProcessingTask() {
        timeLapse = ConfigProperties.getInstance().getPropertyAsLong(ConfigProperties.TIME_LAPSE);
        postProcessing = PostProcessing.getInstance();
    }

    public void start() {
        if(RESTLoader.getInstance().getLoadingJobs() != 0)
            return;

        long lastJob = RESTLoader.getInstance().getLastJobFinished();
        long currentTime = System.currentTimeMillis();
        if((currentTime - lastJob) >= timeLapse)
            postProcessing.run();
    }

    public void stop() {
        postProcessing.stop();
    }

    public Map<String, Object> pause() {
        LOG.warn(this.getClass().getName()+".pause() is not implemented.");
        return new HashMap<>();
    }

    public void resume(Map<String, Object> m) {
        LOG.warn(this.getClass().getName()+".resume() is not implemented.");
    }

    public void restart() {
        postProcessing.reset();
    }
}
