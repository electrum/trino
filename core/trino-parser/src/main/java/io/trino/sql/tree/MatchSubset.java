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

public final class MatchSubset
        extends Statement
{
    private final Identifier variable;
    private final List<Identifier> grouping;

    public MatchSubset(Identifier variable, List<Identifier> grouping)
    {
        this(Optional.empty(), variable, grouping);
    }

    public MatchSubset(NodeLocation location, Identifier variable, List<Identifier> grouping)
    {
        this(Optional.of(location), variable, grouping);
    }

    public MatchSubset(Optional<NodeLocation> location, Identifier variable, List<Identifier> grouping)
    {
        super(location);
        this.variable = requireNonNull(variable, "variable is null");
        this.grouping = requireNonNull(grouping, "grouping is null");
    }

    public Identifier getVariable()
    {
        return variable;
    }

    public List<Identifier> getGrouping()
    {
        return grouping;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMatchSubset(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
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
        MatchSubset other = (MatchSubset) obj;
        return Objects.equals(variable, other.variable) &&
                Objects.equals(grouping, other.grouping);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(variable, grouping);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("variable", variable)
                .add("grouping", grouping)
                .toString();
    }
}
