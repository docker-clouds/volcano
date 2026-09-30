/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package crossqueue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

func setupSession(jobs map[api.JobID]*api.JobInfo) *framework.Session {
	enableReclaim := true
	ssn := &framework.Session{
		Jobs: jobs,
		Tiers: []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{Name: PluginName, EnabledReclaimable: &enableReclaim},
				},
			},
		},
	}
	plugin := New(framework.Arguments{})
	plugin.OnSessionOpen(ssn)
	return ssn
}

func makeTask(jobID api.JobID, name string) *api.TaskInfo {
	return &api.TaskInfo{
		UID:       api.TaskID(name),
		Job:       jobID,
		Name:      name,
		Namespace: "default",
		Pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				UID:       types.UID(name),
			},
		},
	}
}

func makeJob(jobID api.JobID, queue string, priority int32) *api.JobInfo {
	job := api.NewJobInfo(jobID)
	job.Queue = api.QueueID(queue)
	job.Priority = priority
	return job
}

func TestReclaimable_HigherPriorityEvictsLower(t *testing.T) {
	reclaimerID := api.JobID("default/reclaimer-job")
	victimID := api.JobID("default/victim-job")

	jobs := map[api.JobID]*api.JobInfo{
		reclaimerID: makeJob(reclaimerID, "q1", 1000),
		victimID:    makeJob(victimID, "q2", 100),
	}

	ssn := setupSession(jobs)
	reclaimer := makeTask(reclaimerID, "reclaimer")
	victim := makeTask(victimID, "victim")
	reclaimees := []*api.TaskInfo{victim}

	victims := ssn.Reclaimable(reclaimer, reclaimees)
	assert.Len(t, victims, 1)
	assert.Equal(t, victim.UID, victims[0].UID)
}

func TestReclaimable_SamePriorityAbstains(t *testing.T) {
	reclaimerID := api.JobID("default/reclaimer-job")
	victimID := api.JobID("default/victim-job")

	jobs := map[api.JobID]*api.JobInfo{
		reclaimerID: makeJob(reclaimerID, "q1", 500),
		victimID:    makeJob(victimID, "q2", 500),
	}

	ssn := setupSession(jobs)
	reclaimer := makeTask(reclaimerID, "reclaimer")
	victim := makeTask(victimID, "victim")
	reclaimees := []*api.TaskInfo{victim}

	victims := ssn.Reclaimable(reclaimer, reclaimees)
	assert.Empty(t, victims)
}

func TestReclaimable_LowerPriorityReclaimerAbstains(t *testing.T) {
	reclaimerID := api.JobID("default/reclaimer-job")
	victimID := api.JobID("default/victim-job")

	jobs := map[api.JobID]*api.JobInfo{
		reclaimerID: makeJob(reclaimerID, "q1", 100),
		victimID:    makeJob(victimID, "q2", 1000),
	}

	ssn := setupSession(jobs)
	reclaimer := makeTask(reclaimerID, "reclaimer")
	victim := makeTask(victimID, "victim")
	reclaimees := []*api.TaskInfo{victim}

	victims := ssn.Reclaimable(reclaimer, reclaimees)
	assert.Empty(t, victims)
}

func TestReclaimable_MixedPriorities(t *testing.T) {
	reclaimerID := api.JobID("default/reclaimer-job")
	lowID := api.JobID("default/low-job")
	sameID := api.JobID("default/same-job")
	highID := api.JobID("default/high-job")

	jobs := map[api.JobID]*api.JobInfo{
		reclaimerID: makeJob(reclaimerID, "q1", 500),
		lowID:       makeJob(lowID, "q2", 100),
		sameID:      makeJob(sameID, "q3", 500),
		highID:      makeJob(highID, "q4", 1000),
	}

	ssn := setupSession(jobs)
	reclaimer := makeTask(reclaimerID, "reclaimer")
	lowVictim := makeTask(lowID, "low-task")
	sameVictim := makeTask(sameID, "same-task")
	highVictim := makeTask(highID, "high-task")
	reclaimees := []*api.TaskInfo{lowVictim, sameVictim, highVictim}

	victims := ssn.Reclaimable(reclaimer, reclaimees)
	assert.Len(t, victims, 1)
	assert.Equal(t, lowVictim.UID, victims[0].UID)
}

func TestReclaimable_NilReclaimerJobAbstains(t *testing.T) {
	victimID := api.JobID("default/victim-job")

	jobs := map[api.JobID]*api.JobInfo{
		victimID: makeJob(victimID, "q2", 100),
	}

	ssn := setupSession(jobs)
	reclaimer := makeTask("default/nonexistent-job", "reclaimer")
	victim := makeTask(victimID, "victim")
	reclaimees := []*api.TaskInfo{victim}

	victims := ssn.Reclaimable(reclaimer, reclaimees)
	assert.Empty(t, victims)
}
