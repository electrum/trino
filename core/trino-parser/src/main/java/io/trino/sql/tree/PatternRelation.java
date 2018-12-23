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

public final class PatternRelation
        extends Relation
{
    private final Relation relation;
    private final MatchRecognize matchRecognize;
    private final Optional<Identifier> inputName;
    private final List<Identifier> inputColumnNames;
    private final Optional<Identifier> outputName;
    private final List<Identifier> outputColumnNames;

    public PatternRelation(
            Optional<NodeLocation> location,
            Relation relation,
            MatchRecognize matchRecognize,
            Optional<Identifier> inputName,
            List<Identifier> inputColumnNames,
            Optional<Identifier> outputName,
            List<Identifier> outputColumnNames)
    {
        super(location);
        this.relation = requireNonNull(relation, "relation is null");
        this.matchRecognize = requireNonNull(matchRecognize, "matchRecognize is null");
        this.inputName = requireNonNull(inputName, "inputName is null");
        this.inputColumnNames = ImmutableList.copyOf(requireNonNull(inputColumnNames, "inputColumnNames is null"));
        this.outputName = requireNonNull(outputName, "outputName is null");
        this.outputColumnNames = ImmutableList.copyOf(requireNonNull(outputColumnNames, "outputColumnNames is null"));
    }

    public Relation getRelation()
    {
        return relation;
    }

    public MatchRecognize getMatchRecognize()
    {
        return matchRecognize;
    }

    public Optional<Identifier> getInputName()
    {
        return inputName;
    }

    public List<Identifier> getInputColumnNames()
    {
        return inputColumnNames;
    }

    public Optional<Identifier> getOutputName()
    {
        return outputName;
    }

    public List<Identifier> getOutputColumnNames()
    {
        return outputColumnNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPatternRelation(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(relation, matchRecognize);
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

        PatternRelation other = (PatternRelation) obj;
        return Objects.equals(relation, other.relation) &&
                Objects.equals(matchRecognize, other.matchRecognize) &&
                Objects.equals(inputName, other.inputName) &&
                Objects.equals(inputColumnNames, other.inputColumnNames) &&
                Objects.equals(outputName, other.outputName) &&
                Objects.equals(outputColumnNames, other.outputColumnNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relation, matchRecognize, inputName, inputColumnNames, outputName, outputColumnNames);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relation", relation)
                .add("matchRecognize", matchRecognize)
                .add("inputName", inputName.orElse(null))
                .add("inputColumnNames", inputColumnNames.isEmpty() ? null : inputColumnNames)
                .add("outputName", outputName.orElse(null))
                .add("outputColumnNames", outputColumnNames.isEmpty() ? null : outputColumnNames)
                .omitNullValues()
                .toString();
    }
}
