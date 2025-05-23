/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.rest.messages.json.JobIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobIDSerializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** Details about a job. */
public class JobDetailsInfo implements ResponseBody {

    public static final String FIELD_NAME_JOB_ID = "jid";

    public static final String FIELD_NAME_JOB_NAME = "name";

    public static final String FIELD_NAME_IS_STOPPABLE = "isStoppable";

    public static final String FIELD_NAME_JOB_STATUS = "state";

    public static final String FIELD_NAME_JOB_TYPE = "job-type";

    public static final String FIELD_NAME_START_TIME = "start-time";

    public static final String FIELD_NAME_END_TIME = "end-time";

    public static final String FIELD_NAME_DURATION = "duration";

    public static final String FIELD_NAME_MAX_PARALLELISM = "maxParallelism";

    // TODO: For what do we need this???
    public static final String FIELD_NAME_NOW = "now";

    public static final String FIELD_NAME_TIMESTAMPS = "timestamps";

    public static final String FIELD_NAME_JOB_VERTEX_INFOS = "vertices";

    public static final String FIELD_NAME_JOB_VERTICES_PER_STATE = "status-counts";

    public static final String FIELD_NAME_JSON_PLAN = "plan";

    /**
     * The {@link JobPlanInfo.RawJson} of the submitted stream graph, or null if the job is
     * submitted with a JobGraph or if it's a streaming job.
     */
    public static final String FIELD_NAME_STREAM_GRAPH_JSON = "stream-graph";

    public static final String FIELD_NAME_PENDING_OPERATORS = "pending-operators";

    @JsonProperty(FIELD_NAME_JOB_ID)
    @JsonSerialize(using = JobIDSerializer.class)
    private final JobID jobId;

    @JsonProperty(FIELD_NAME_JOB_NAME)
    private final String name;

    @JsonProperty(FIELD_NAME_IS_STOPPABLE)
    private final boolean isStoppable;

    @JsonProperty(FIELD_NAME_JOB_STATUS)
    private final JobStatus jobStatus;

    @JsonProperty(FIELD_NAME_JOB_TYPE)
    private final JobType jobType;

    @JsonProperty(FIELD_NAME_START_TIME)
    private final long startTime;

    @JsonProperty(FIELD_NAME_END_TIME)
    private final long endTime;

    @JsonProperty(FIELD_NAME_DURATION)
    private final long duration;

    @JsonProperty(FIELD_NAME_MAX_PARALLELISM)
    private final long maxParallelism;

    @JsonProperty(FIELD_NAME_NOW)
    private final long now;

    @JsonProperty(FIELD_NAME_TIMESTAMPS)
    private final Map<JobStatus, Long> timestamps;

    @JsonProperty(FIELD_NAME_JOB_VERTEX_INFOS)
    private final Collection<JobVertexDetailsInfo> jobVertexInfos;

    @JsonProperty(FIELD_NAME_JOB_VERTICES_PER_STATE)
    private final Map<ExecutionState, Integer> jobVerticesPerState;

    @JsonProperty(FIELD_NAME_JSON_PLAN)
    private final JobPlanInfo.RawJson jsonPlan;

    @JsonProperty(FIELD_NAME_STREAM_GRAPH_JSON)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final JobPlanInfo.RawJson streamGraphJson;

    @JsonProperty(FIELD_NAME_PENDING_OPERATORS)
    private final int pendingOperators;

    @JsonCreator
    public JobDetailsInfo(
            @JsonDeserialize(using = JobIDDeserializer.class) @JsonProperty(FIELD_NAME_JOB_ID)
                    JobID jobId,
            @JsonProperty(FIELD_NAME_JOB_NAME) String name,
            @JsonProperty(FIELD_NAME_IS_STOPPABLE) boolean isStoppable,
            @JsonProperty(FIELD_NAME_JOB_STATUS) JobStatus jobStatus,
            @JsonProperty(FIELD_NAME_JOB_TYPE) JobType jobType,
            @JsonProperty(FIELD_NAME_START_TIME) long startTime,
            @JsonProperty(FIELD_NAME_END_TIME) long endTime,
            @JsonProperty(FIELD_NAME_DURATION) long duration,
            @JsonProperty(FIELD_NAME_MAX_PARALLELISM) long maxParallelism,
            @JsonProperty(FIELD_NAME_NOW) long now,
            @JsonProperty(FIELD_NAME_TIMESTAMPS) Map<JobStatus, Long> timestamps,
            @JsonProperty(FIELD_NAME_JOB_VERTEX_INFOS)
                    Collection<JobVertexDetailsInfo> jobVertexInfos,
            @JsonProperty(FIELD_NAME_JOB_VERTICES_PER_STATE)
                    Map<ExecutionState, Integer> jobVerticesPerState,
            @JsonProperty(FIELD_NAME_JSON_PLAN) JobPlanInfo.RawJson jsonPlan,
            @JsonProperty(FIELD_NAME_STREAM_GRAPH_JSON) @Nullable
                    JobPlanInfo.RawJson streamGraphJson,
            @JsonProperty(FIELD_NAME_PENDING_OPERATORS) int pendingOperators) {
        this.jobId = Preconditions.checkNotNull(jobId);
        this.name = Preconditions.checkNotNull(name);
        this.isStoppable = isStoppable;
        this.jobStatus = Preconditions.checkNotNull(jobStatus);
        this.jobType = Preconditions.checkNotNull(jobType);
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
        this.maxParallelism = maxParallelism;
        this.now = now;
        this.timestamps = Preconditions.checkNotNull(timestamps);
        this.jobVertexInfos = Preconditions.checkNotNull(jobVertexInfos);
        this.jobVerticesPerState = Preconditions.checkNotNull(jobVerticesPerState);
        this.jsonPlan = Preconditions.checkNotNull(jsonPlan);
        this.streamGraphJson = streamGraphJson;
        this.pendingOperators = pendingOperators;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobDetailsInfo that = (JobDetailsInfo) o;
        return isStoppable == that.isStoppable
                && startTime == that.startTime
                && endTime == that.endTime
                && duration == that.duration
                && maxParallelism == that.maxParallelism
                && now == that.now
                && Objects.equals(jobId, that.jobId)
                && Objects.equals(name, that.name)
                && jobStatus == that.jobStatus
                && jobType == that.jobType
                && Objects.equals(timestamps, that.timestamps)
                && Objects.equals(jobVertexInfos, that.jobVertexInfos)
                && Objects.equals(jobVerticesPerState, that.jobVerticesPerState)
                && Objects.equals(jsonPlan, that.jsonPlan)
                && Objects.equals(streamGraphJson, that.streamGraphJson)
                && Objects.equals(pendingOperators, that.pendingOperators);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jobId,
                name,
                isStoppable,
                jobStatus,
                jobType,
                startTime,
                endTime,
                duration,
                maxParallelism,
                now,
                timestamps,
                jobVertexInfos,
                jobVerticesPerState,
                jsonPlan,
                streamGraphJson,
                pendingOperators);
    }

    @JsonIgnore
    public JobID getJobId() {
        return jobId;
    }

    @JsonIgnore
    public String getName() {
        return name;
    }

    @JsonIgnore
    public boolean isStoppable() {
        return isStoppable;
    }

    @JsonIgnore
    public JobStatus getJobStatus() {
        return jobStatus;
    }

    @JsonIgnore
    public JobType getJobType() {
        return jobType;
    }

    @JsonIgnore
    public long getStartTime() {
        return startTime;
    }

    @JsonIgnore
    public long getEndTime() {
        return endTime;
    }

    @JsonIgnore
    public long getMaxParallelism() {
        return maxParallelism;
    }

    @JsonIgnore
    public long getDuration() {
        return duration;
    }

    @JsonIgnore
    public long getNow() {
        return now;
    }

    @JsonIgnore
    public Map<JobStatus, Long> getTimestamps() {
        return timestamps;
    }

    @JsonIgnore
    public Collection<JobVertexDetailsInfo> getJobVertexInfos() {
        return jobVertexInfos;
    }

    @JsonIgnore
    public Map<ExecutionState, Integer> getJobVerticesPerState() {
        return jobVerticesPerState;
    }

    @JsonIgnore
    public String getJsonPlan() {
        return jsonPlan.toString();
    }

    @JsonIgnore
    @Nullable
    public String getStreamGraphJson() {
        if (streamGraphJson != null) {
            return streamGraphJson.toString();
        }
        return null;
    }

    @JsonIgnore
    public int getPendingOperators() {
        return pendingOperators;
    }

    // ---------------------------------------------------
    // Static inner classes
    // ---------------------------------------------------

    /** Detailed information about a job vertex. */
    @Schema(name = "JobDetailsVertexInfo")
    public static final class JobVertexDetailsInfo {

        public static final String FIELD_NAME_JOB_VERTEX_ID = "id";

        public static final String FIELD_NAME_SLOT_SHARING_GROUP_ID = "slotSharingGroupId";

        public static final String FIELD_NAME_JOB_VERTEX_NAME = "name";

        public static final String FIELD_NAME_MAX_PARALLELISM = "maxParallelism";

        public static final String FIELD_NAME_PARALLELISM = "parallelism";

        public static final String FIELD_NAME_JOB_VERTEX_STATE = "status";

        public static final String FIELD_NAME_JOB_VERTEX_START_TIME = "start-time";

        public static final String FIELD_NAME_JOB_VERTEX_END_TIME = "end-time";

        public static final String FIELD_NAME_JOB_VERTEX_DURATION = "duration";

        public static final String FIELD_NAME_TASKS_PER_STATE = "tasks";

        public static final String FIELD_NAME_JOB_VERTEX_METRICS = "metrics";

        @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
        @JsonSerialize(using = JobVertexIDSerializer.class)
        private final JobVertexID jobVertexID;

        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
        @JsonSerialize(using = SlotSharingGroupIDSerializer.class)
        private final SlotSharingGroupId slotSharingGroupId;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_NAME)
        private final String name;

        @JsonProperty(FIELD_NAME_MAX_PARALLELISM)
        private final int maxParallelism;

        @JsonProperty(FIELD_NAME_PARALLELISM)
        private final int parallelism;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_STATE)
        private final ExecutionState executionState;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_START_TIME)
        private final long startTime;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_END_TIME)
        private final long endTime;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_DURATION)
        private final long duration;

        @JsonProperty(FIELD_NAME_TASKS_PER_STATE)
        private final Map<ExecutionState, Integer> tasksPerState;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_METRICS)
        private final IOMetricsInfo jobVertexMetrics;

        @JsonCreator
        public JobVertexDetailsInfo(
                @JsonDeserialize(using = JobVertexIDDeserializer.class)
                        @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
                        JobVertexID jobVertexID,
                @JsonDeserialize(using = SlotSharingGroupIDDeserializer.class)
                        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
                        SlotSharingGroupId slotSharingGroupId,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_NAME) String name,
                @JsonProperty(FIELD_NAME_MAX_PARALLELISM) int maxParallelism,
                @JsonProperty(FIELD_NAME_PARALLELISM) int parallelism,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_STATE) ExecutionState executionState,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_START_TIME) long startTime,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_END_TIME) long endTime,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_DURATION) long duration,
                @JsonProperty(FIELD_NAME_TASKS_PER_STATE)
                        Map<ExecutionState, Integer> tasksPerState,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_METRICS) IOMetricsInfo jobVertexMetrics) {
            this.jobVertexID = Preconditions.checkNotNull(jobVertexID);
            this.slotSharingGroupId = Preconditions.checkNotNull(slotSharingGroupId);
            this.name = Preconditions.checkNotNull(name);
            this.maxParallelism = maxParallelism;
            this.parallelism = parallelism;
            this.executionState = Preconditions.checkNotNull(executionState);
            this.startTime = startTime;
            this.endTime = endTime;
            this.duration = duration;
            this.tasksPerState = Preconditions.checkNotNull(tasksPerState);
            this.jobVertexMetrics = Preconditions.checkNotNull(jobVertexMetrics);
        }

        @JsonIgnore
        public JobVertexID getJobVertexID() {
            return jobVertexID;
        }

        @JsonIgnore
        public SlotSharingGroupId getSlotSharingGroupId() {
            return slotSharingGroupId;
        }

        @JsonIgnore
        public String getName() {
            return name;
        }

        @JsonIgnore
        public int getMaxParallelism() {
            return maxParallelism;
        }

        @JsonIgnore
        public int getParallelism() {
            return parallelism;
        }

        @JsonIgnore
        public ExecutionState getExecutionState() {
            return executionState;
        }

        @JsonIgnore
        public long getStartTime() {
            return startTime;
        }

        @JsonIgnore
        public long getEndTime() {
            return endTime;
        }

        @JsonIgnore
        public long getDuration() {
            return duration;
        }

        @JsonIgnore
        public Map<ExecutionState, Integer> getTasksPerState() {
            return tasksPerState;
        }

        @JsonIgnore
        public IOMetricsInfo getJobVertexMetrics() {
            return jobVertexMetrics;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JobVertexDetailsInfo that = (JobVertexDetailsInfo) o;
            return maxParallelism == that.maxParallelism
                    && parallelism == that.parallelism
                    && startTime == that.startTime
                    && endTime == that.endTime
                    && duration == that.duration
                    && Objects.equals(jobVertexID, that.jobVertexID)
                    && Objects.equals(slotSharingGroupId, that.slotSharingGroupId)
                    && Objects.equals(name, that.name)
                    && executionState == that.executionState
                    && Objects.equals(tasksPerState, that.tasksPerState)
                    && Objects.equals(jobVertexMetrics, that.jobVertexMetrics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    jobVertexID,
                    slotSharingGroupId,
                    name,
                    maxParallelism,
                    parallelism,
                    executionState,
                    startTime,
                    endTime,
                    duration,
                    tasksPerState,
                    jobVertexMetrics);
        }
    }
}
