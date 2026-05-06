/*
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
package io.trino.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.util.MachineInfo.calculateAvailablePhysicalProcessorCount;
import static io.trino.util.MachineInfo.hasOnlyRootCgroups;
import static io.trino.util.MachineInfo.parseLinuxCpuFlags;
import static io.trino.util.MachineInfo.parseLinuxPhysicalProcessorCount;
import static io.trino.util.MachineInfo.parseLinuxSmtActive;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMachineInfo
{
    @Test
    void testCalculateAvailablePhysicalProcessorCount()
    {
        assertThat(calculateAvailablePhysicalProcessorCount(16, true, Optional.of(true), () -> Optional.of(8))).isEqualTo(8);
        assertThat(calculateAvailablePhysicalProcessorCount(4, true, Optional.of(true), () -> Optional.of(8))).isEqualTo(4);
        assertThat(calculateAvailablePhysicalProcessorCount(16, true, Optional.empty(), () -> Optional.of(8))).isEqualTo(8);
        assertThat(calculateAvailablePhysicalProcessorCount(16, true, Optional.of(true), Optional::empty)).isEqualTo(16);
    }

    @Test
    void testCalculateAvailablePhysicalProcessorCountInNonRootCgroup()
    {
        AtomicBoolean physicalProcessorCountRead = new AtomicBoolean();

        assertThat(calculateAvailablePhysicalProcessorCount(16, false, Optional.of(true), () -> {
            physicalProcessorCountRead.set(true);
            return Optional.of(8);
        })).isEqualTo(16);
        assertThat(physicalProcessorCountRead).isFalse();
    }

    @Test
    void testCalculateAvailablePhysicalProcessorCountWithSmtDisabled()
    {
        AtomicBoolean physicalProcessorCountRead = new AtomicBoolean();

        assertThat(calculateAvailablePhysicalProcessorCount(16, true, Optional.of(false), () -> {
            physicalProcessorCountRead.set(true);
            return Optional.of(8);
        })).isEqualTo(16);
        assertThat(physicalProcessorCountRead).isFalse();
    }

    @Test
    void testParseLinuxSmtActive()
    {
        assertThat(parseLinuxSmtActive("1")).isEqualTo(Optional.of(true));
        assertThat(parseLinuxSmtActive("0\n")).isEqualTo(Optional.of(false));
        assertThat(parseLinuxSmtActive("unknown")).isEqualTo(Optional.empty());
    }

    @Test
    void testHasOnlyRootCgroups()
    {
        assertThat(hasOnlyRootCgroups(List.of("0::/"))).isTrue();
        assertThat(hasOnlyRootCgroups(List.of("2:cpu,cpuacct:/", "1:name=systemd:/"))).isTrue();
        assertThat(hasOnlyRootCgroups(List.of("0::/user.slice/user-1000.slice/session-1.scope"))).isFalse();
        assertThat(hasOnlyRootCgroups(List.of("0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod123.slice/cri-containerd-456.scope"))).isFalse();
        assertThat(hasOnlyRootCgroups(List.of("1:name=systemd:/docker/0123456789abcdef"))).isFalse();
        assertThat(hasOnlyRootCgroups(List.of("invalid"))).isFalse();
        assertThat(hasOnlyRootCgroups(List.of())).isFalse();
    }

    @Test
    void testParseLinuxPhysicalProcessorCount()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                physical id : 0
                core id : 0

                processor : 1
                physical id : 0
                core id : 0

                processor : 2
                physical id : 0
                core id : 1

                processor : 3
                physical id : 0
                core id : 1
                """.lines().toList();

        assertThat(parseLinuxPhysicalProcessorCount(cpuInfoLines))
                .isEqualTo(Optional.of(2));
    }

    @Test
    void testParseLinuxPhysicalProcessorCountWithoutCoreIds()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                flags : avx512f avx512_vbmi2

                processor : 1
                flags : avx512f avx512_vbmi2
                """.lines().toList();

        assertThat(parseLinuxPhysicalProcessorCount(cpuInfoLines))
                .isEqualTo(Optional.empty());
    }

    @Test
    void testParseLinuxCpuFlagsIntersection()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                flags : avx512f avx512_vbmi2 asimd

                processor : 1
                flags : avx512f asimd
                """.lines().toList();

        assertThat(parseLinuxCpuFlags(cpuInfoLines))
                .isEqualTo(Set.of("avx512f", "neon"));
    }

    @Test
    void testParseLinuxCpuFlagsSingleProcessor()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                flags : avx512f asimd
                """.lines().toList();

        assertThat(parseLinuxCpuFlags(cpuInfoLines))
                .isEqualTo(Set.of("avx512f", "neon"));
    }
}
