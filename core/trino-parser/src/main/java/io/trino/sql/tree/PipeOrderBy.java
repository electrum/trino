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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class PipeOrderBy
        extends PipeOperator
{
    private final OrderBy orderBy;

    public PipeOrderBy(NodeLocation location, OrderBy orderBy)
    {
        super(location);
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
    }

    public OrderBy getOrderBy()
    {
        return orderBy;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPipeOrderBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(orderBy);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(orderBy)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orderBy);
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof PipeOrderBy other) &&
                orderBy.equals(other.orderBy);
    }
}
