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

public final class MatchRecognize
        extends Statement
{
    private final List<Expression> partitionBy;
    private final Optional<OrderBy> orderBy;
    private final List<MatchMeasure> measures;
    private final Optional<RowsPerMatch> rowsPerMatch;
    private final Optional<MatchSkip> skip;
    private final RowPattern pattern;
    private final List<MatchSubset> subsets;
    private final List<MatchDefinition> definitions;

    public MatchRecognize(
            Optional<NodeLocation> location,
            List<Expression> partitionBy,
            Optional<OrderBy> orderBy,
            List<MatchMeasure> measures,
            Optional<RowsPerMatch> rowsPerMatch,
            Optional<MatchSkip> skip, RowPattern pattern,
            List<MatchSubset> subsets,
            List<MatchDefinition> definitions)
    {
        super(location);
        this.partitionBy = ImmutableList.copyOf(requireNonNull(partitionBy, "partitionBy is null"));
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.measures = ImmutableList.copyOf(requireNonNull(measures, "measures is null"));
        this.rowsPerMatch = requireNonNull(rowsPerMatch, "rowsPerMatch is null");
        this.skip = requireNonNull(skip, "skip is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.subsets = ImmutableList.copyOf(requireNonNull(subsets, "subsets is null"));
        this.definitions = ImmutableList.copyOf(requireNonNull(definitions, "definitions is null"));
    }

    public List<Expression> getPartitionBy()
    {
        return partitionBy;
    }

    public Optional<OrderBy> getOrderBy()
    {
        return orderBy;
    }

    public List<MatchMeasure> getMeasures()
    {
        return measures;
    }

    public Optional<RowsPerMatch> getRowsPerMatch()
    {
        return rowsPerMatch;
    }

    public Optional<MatchSkip> getSkip()
    {
        return skip;
    }

    public RowPattern getPattern()
    {
        return pattern;
    }

    public List<MatchSubset> getSubsets()
    {
        return subsets;
    }

    public List<MatchDefinition> getDefinitions()
    {
        return definitions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMatchRecognize(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(partitionBy)
                .addAll(orderBy.map(ImmutableList::of).orElse(ImmutableList.of()))
                .addAll(measures)
                .addAll(rowsPerMatch.map(ImmutableList::of).orElse(ImmutableList.of()))
                .addAll(skip.map(ImmutableList::of).orElse(ImmutableList.of()))
                .add(pattern)
                .addAll(subsets)
                .addAll(definitions)
                .build();
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
        MatchRecognize other = (MatchRecognize) obj;
        return Objects.equals(partitionBy, other.partitionBy) &&
                Objects.equals(orderBy, other.orderBy) &&
                Objects.equals(measures, other.measures) &&
                Objects.equals(rowsPerMatch, other.rowsPerMatch) &&
                Objects.equals(skip, other.skip) &&
                Objects.equals(pattern, other.pattern) &&
                Objects.equals(subsets, other.subsets) &&
                Objects.equals(definitions, other.definitions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionBy, orderBy, measures, rowsPerMatch, skip, pattern, subsets, definitions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionBy", partitionBy)
                .add("orderBy", orderBy)
                .add("measures", measures)
                .add("rowsPerMatch", rowsPerMatch)
                .add("skip", skip)
                .add("pattern", pattern)
                .add("subsets", subsets)
                .add("definitions", definitions)
                .toString();
    }
}
