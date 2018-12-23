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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class PermutedRowPattern
        extends RowPattern
{
    private final List<RowPattern> patterns;

    public PermutedRowPattern(List<RowPattern> patterns)
    {
        this(Optional.empty(), patterns);
    }

    public PermutedRowPattern(NodeLocation location, List<RowPattern> patterns)
    {
        this(Optional.of(location), patterns);
    }

    private PermutedRowPattern(Optional<NodeLocation> location, List<RowPattern> patterns)
    {
        super(location);
        this.patterns = ImmutableList.copyOf(requireNonNull(patterns, "patterns is null"));
    }

    public List<RowPattern> getPatterns()
    {
        return patterns;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPermutedRowPattern(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return patterns;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PermutedRowPattern other = (PermutedRowPattern) obj;
        return Objects.equals(patterns, other.patterns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(patterns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("patterns", patterns)
                .toString();
    }
}
