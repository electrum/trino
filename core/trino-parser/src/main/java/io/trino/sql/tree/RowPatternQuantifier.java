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
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public final class RowPatternQuantifier
        extends Node
{
    public static RowPatternQuantifier zeroOrMore(Optional<NodeLocation> location, boolean reluctant)
    {
        return new RowPatternQuantifier(location, OptionalInt.of(0), OptionalInt.empty(), reluctant, Optional.of("*"));
    }

    public static RowPatternQuantifier oneOrMore(Optional<NodeLocation> location, boolean reluctant)
    {
        return new RowPatternQuantifier(location, OptionalInt.of(1), OptionalInt.empty(), reluctant, Optional.of("+"));
    }

    public static RowPatternQuantifier zeroOrOne(Optional<NodeLocation> location, boolean reluctant)
    {
        return new RowPatternQuantifier(location, OptionalInt.of(0), OptionalInt.of(1), reluctant, Optional.of("?"));
    }

    public static RowPatternQuantifier exact(Optional<NodeLocation> location, int count)
    {
        return new RowPatternQuantifier(location, OptionalInt.of(count), OptionalInt.of(count), false, Optional.of("{" + count + "}"));
    }

    public static RowPatternQuantifier atLeast(Optional<NodeLocation> location, int min, boolean reluctant)
    {
        return new RowPatternQuantifier(location, OptionalInt.of(min), OptionalInt.empty(), reluctant, Optional.empty());
    }

    public static RowPatternQuantifier atMost(Optional<NodeLocation> location, int max, boolean reluctant)
    {
        return new RowPatternQuantifier(location, OptionalInt.empty(), OptionalInt.of(max), reluctant, Optional.empty());
    }

    public static RowPatternQuantifier between(Optional<NodeLocation> location, int min, int max, boolean reluctant)
    {
        return new RowPatternQuantifier(location, OptionalInt.of(min), OptionalInt.of(max), reluctant, Optional.empty());
    }

    private final OptionalInt min;
    private final OptionalInt max;
    private final boolean reluctant;
    private final Optional<String> canonical;

    private RowPatternQuantifier(Optional<NodeLocation> location, OptionalInt min, OptionalInt max, boolean reluctant, Optional<String> canonical)
    {
        super(location);
        this.min = requireNonNull(min, "min is null");
        this.max = requireNonNull(max, "max is null");
        this.reluctant = reluctant;
        this.canonical = requireNonNull(canonical, "canonical is null");
    }

    public OptionalInt getMin()
    {
        return min;
    }

    public OptionalInt getMax()
    {
        return max;
    }

    public boolean isReluctant()
    {
        return reluctant;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRowPatternQuantifier(this, context);
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
        RowPatternQuantifier other = (RowPatternQuantifier) obj;
        return Objects.equals(min, other.min) &&
                Objects.equals(max, other.max) &&
                Objects.equals(reluctant, other.reluctant) &&
                Objects.equals(canonical, other.canonical);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(min, max, reluctant, canonical);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        if (canonical.isPresent()) {
            builder.append(canonical.get());
        }
        else {
            builder.append('{');
            min.ifPresent(builder::append);
            builder.append(',');
            max.ifPresent(builder::append);
            builder.append('}');
        }

        if (reluctant) {
            builder.append('?');
        }

        return builder.toString();
    }
}
