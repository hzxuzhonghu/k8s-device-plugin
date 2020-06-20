/*
 * Copyright (c) 2019, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nvidia

const (
	// GPU card core num
	GpuCoreConst = "volcano.sh/gpu-core"

	VCoreAnnotationConst         = "volcano.sh/vcuda-core"
	VMemoryAnnotationConst       = "volcano.sh/vcuda-memory"
	PredicateTimeAnnotationConst = "volcano.sh/predicate-time"
	GPUAssignedConst             = "volcano.sh/gpu-assigned"

	AssumedTimeEnv       = "assumed_time_env"
	ResourceIndexEnv     = "resource_index_env"
	ContainerResourceEnv = "container_resource_env"
)
