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

public final class PipeSelect
        extends PipeOperator
{
    private final Select select;

    public PipeSelect(NodeLocation location, Select select)
    {
        super(location);
        this.select = requireNonNull(select, "select is null");
    }

    public Select getSelect()
    {
        return select;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPipeSelect(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(select);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(select)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(select);
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof PipeSelect other) &&
                select.equals(other.select);
    }
}
