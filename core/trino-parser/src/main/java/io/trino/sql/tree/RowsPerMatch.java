

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

public final class RowsPerMatch
        extends Node
{
    public enum Type
    {
        ONE_ROW("ONE ROW PER MATCH"),
        ALL_ROWS("ALL ROWS PER MATCH"),
        ALL_ROWS_SHOW_EMPTY("ALL ROWS PER MATCH SHOW EMPTY MATCHES"),
        ALL_ROWS_OMIT_EMPTY("ALL ROWS PER MATCH OMIT EMPTY MATCHES"),
        ALL_ROWS_WITH_UNMATCHED("ALL ROWS PER MATCH WITH UNMATCHED ROWS");

        private final String text;

        Type(String text)
        {
            this.text = requireNonNull(text, "text is null");
        }

        public String getText()
        {
            return text;
        }
    }

    private final Type type;

    public RowsPerMatch(Type type)
    {
        this(Optional.empty(), type);
    }

    public RowsPerMatch(NodeLocation location, Type type)
    {
        this(Optional.of(location), type);
    }

    private RowsPerMatch(Optional<NodeLocation> location, Type type)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRowsPerMatch(this, context);
    }

    @Override
    public List<Node> getChildren()
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
        RowsPerMatch other = (RowsPerMatch) obj;
        return type == other.type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
