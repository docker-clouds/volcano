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
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const PluginName = "crossqueue"

type crossQueuePlugin struct {
	pluginArguments framework.Arguments
}

func New(arguments framework.Arguments) framework.Plugin {
	return &crossQueuePlugin{pluginArguments: arguments}
}

func (cp *crossQueuePlugin) Name() string {
	return PluginName
}

func (cp *crossQueuePlugin) OnSessionOpen(ssn *framework.Session) {
	reclaimableFn := func(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		reclaimerJob := ssn.Jobs[reclaimer.Job]
		if reclaimerJob == nil {
			klog.Warningf("[crossqueue] Reclaimer job <%s> not found in session", reclaimer.Job)
			return nil, util.Abstain
		}

		var victims []*api.TaskInfo
		for _, reclaimee := range reclaimees {
			reclaimeeJob := ssn.Jobs[reclaimee.Job]
			if reclaimeeJob == nil {
				klog.Warningf("[crossqueue] Skip reclaimee <%s/%s>: job <%s> not found in session",
					reclaimee.Namespace, reclaimee.Name, reclaimee.Job)
				continue
			}

			if reclaimeeJob.Priority < reclaimerJob.Priority {
				victims = append(victims, reclaimee)
				klog.V(3).Infof("[crossqueue] Allow cross-queue reclaim: reclaimer job <%s> priority <%d> "+
					"can preempt task <%s/%s> from job <%s> priority <%d> in queue <%s>",
					reclaimerJob.Name, reclaimerJob.Priority,
					reclaimee.Namespace, reclaimee.Name,
					reclaimeeJob.Name, reclaimeeJob.Priority, reclaimeeJob.Queue)
			} else {
				klog.V(4).Infof("[crossqueue] Skip task <%s/%s>: job priority <%d> >= reclaimer priority <%d>",
					reclaimee.Namespace, reclaimee.Name, reclaimeeJob.Priority, reclaimerJob.Priority)
			}
		}

		if len(victims) == 0 {
			klog.V(4).Infof("[crossqueue] No lower-priority cross-queue victims for reclaimer job <%s> (priority <%d>), "+
				"abstaining to let fairness-based reclaim decide",
				reclaimerJob.Name, reclaimerJob.Priority)
			return nil, util.Abstain
		}

		klog.V(3).Infof("[crossqueue] Found <%d> lower-priority cross-queue victim(s) for reclaimer job <%s>",
			len(victims), reclaimerJob.Name)
		return victims, util.Permit
	}
	ssn.AddReclaimableFn(cp.Name(), reclaimableFn)
}

func (cp *crossQueuePlugin) OnSessionClose(ssn *framework.Session) {}
