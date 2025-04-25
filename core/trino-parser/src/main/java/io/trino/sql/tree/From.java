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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class From
        extends QueryBody
{
    private final Relation relation;

    public From(NodeLocation location, Relation relation)
    {
        super(Optional.of(location));
        this.relation = requireNonNull(relation, "relation is null");
    }

    public Relation getRelation()
    {
        return relation;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFrom(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(relation);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(relation)
                .toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof From other) &&
                relation.equals(other.relation);
    }

    @Override
    public int hashCode()
    {
        return relation.hashCode();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
