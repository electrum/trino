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

public final class PipeLimit
        extends PipeOperator
{
    private final Limit limit;
    private final Optional<Offset> offset;

    public PipeLimit(NodeLocation location, Limit limit, Optional<Offset> offset)
    {
        super(location);
        this.limit = requireNonNull(limit, "limit is null");
        this.offset = requireNonNull(offset, "offset is null");
    }

    public Limit getLimit()
    {
        return limit;
    }

    public Optional<Offset> getOffset()
    {
        return offset;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPipeLimit(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(limit)
                .addAll(offset.stream().toList())
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(limit)
                .addValue(offset.orElse(null))
                .omitNullValues()
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(limit);
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof PipeLimit other) &&
                limit.equals(other.limit);
    }
}
