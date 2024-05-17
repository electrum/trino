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
package io.trino.plugin.functions.python;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.HALF_UP;

final class TrinoTypes
{
    private TrinoTypes() {}

    public static void validateReturnType(Type type)
    {
        switch (type) {
            case RowType rowType -> {
                for (RowType.Field field : rowType.getFields()) {
                    validateReturnType(field.getType());
                }
            }
            case ArrayType arrayType -> validateReturnType(arrayType.getElementType());
            case MapType mapType -> {
                validateReturnType(mapType.getKeyType());
                validateReturnType(mapType.getValueType());
            }
            case VarcharType varcharType -> {
                if (!varcharType.isUnbounded()) {
                    throw new TrinoException(NOT_SUPPORTED, "Type VARCHAR(x) not supported as return type. Use VARCHAR instead.");
                }
            }
            case CharType _ -> throw new TrinoException(NOT_SUPPORTED, "Type CHAR not supported as return type. Use VARCHAR instead.");
            default -> {
                if (type.getBaseName().equals(StandardTypes.JSON)) {
                    throw new TrinoException(NOT_SUPPORTED, "Type JSON not supported as return type. Use VARCHAR instead.");
                }
            }
        }
    }

    public static Slice toTypeDescriptor(Type type)
    {
        SliceOutput output = new DynamicSliceOutput(64);
        toTypeDescriptor(type, output);
//        System.out.println(HexFormat.ofDelimiter(" ").formatHex(output.slice().getBytes()));
        return output.slice();
    }

    private static void toTypeDescriptor(Type type, SliceOutput output)
    {
        switch (type) {
            case RowType rowType -> {
                output.writeInt(TrinoType.ROW.id());
                output.writeInt(rowType.getFields().size());
                for (RowType.Field field : rowType.getFields()) {
                    toTypeDescriptor(field.getType(), output);
                }
            }
            case ArrayType arrayType -> {
                output.writeInt(TrinoType.ARRAY.id());
                toTypeDescriptor(arrayType.getElementType(), output);
            }
            case MapType mapType -> {
                output.writeInt(TrinoType.MAP.id());
                toTypeDescriptor(mapType.getKeyType(), output);
                toTypeDescriptor(mapType.getValueType(), output);
            }
            case DecimalType decimalType -> {
                output.writeInt(TrinoType.DECIMAL.id());
                output.writeInt(decimalType.getPrecision());
                output.writeInt(decimalType.getScale());
            }
            case VarcharType varcharType -> {
                output.writeInt(TrinoType.VARCHAR.id());
                output.writeInt(varcharType.getLength().orElse(VarcharType.MAX_LENGTH));
            }
            case CharType charType -> {
                output.writeInt(TrinoType.CHAR.id());
                output.writeInt(charType.getLength());
            }
            case TimeType timeType -> {
                output.writeInt(TrinoType.TIME.id());
                output.writeInt(timeType.getPrecision());
            }
            case TimeWithTimeZoneType timeType -> {
                output.writeInt(TrinoType.TIME_WITH_TIME_ZONE.id());
                output.writeInt(timeType.getPrecision());
            }
            case TimestampType timestampType -> {
                output.writeInt(TrinoType.TIMESTAMP.id());
                output.writeInt(timestampType.getPrecision());
            }
            case TimestampWithTimeZoneType timestampType -> {
                output.writeInt(TrinoType.TIMESTAMP_WITH_TIME_ZONE.id());
                output.writeInt(timestampType.getPrecision());
            }
            default -> output.writeInt(singletonType(type).id());
        }
    }

    private static TrinoType singletonType(Type type)
    {
        return switch (type.getBaseName()) {
            case StandardTypes.BOOLEAN -> TrinoType.BOOLEAN;
            case StandardTypes.BIGINT -> TrinoType.BIGINT;
            case StandardTypes.INTEGER -> TrinoType.INTEGER;
            case StandardTypes.SMALLINT -> TrinoType.SMALLINT;
            case StandardTypes.TINYINT -> TrinoType.TINYINT;
            case StandardTypes.DOUBLE -> TrinoType.DOUBLE;
            case StandardTypes.REAL -> TrinoType.REAL;
            case StandardTypes.VARBINARY -> TrinoType.VARBINARY;
            case StandardTypes.DATE -> TrinoType.DATE;
            case StandardTypes.INTERVAL_YEAR_TO_MONTH -> TrinoType.INTERVAL_YEAR_TO_MONTH;
            case StandardTypes.INTERVAL_DAY_TO_SECOND -> TrinoType.INTERVAL_DAY_TO_SECOND;
            case StandardTypes.JSON -> TrinoType.JSON;
            case StandardTypes.UUID -> TrinoType.UUID;
            case StandardTypes.IPADDRESS -> TrinoType.IPADDRESS;
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        };
    }

    public static Slice javaToBinary(List<Type> types, Object[] values)
    {
        SliceOutput output = new DynamicSliceOutput(64);
        output.writeByte(1); // row present
        for (int i = 0; i < types.size(); i++) {
            javaToBinary(types.get(i), values[i], output);
        }
//        System.out.println(HexFormat.ofDelimiter(" ").formatHex(output.slice().getBytes()));
        return output.slice();
    }

    private static void javaToBinary(Type type, Object value, SliceOutput output)
    {
        if (value == null) {
            output.writeByte(0);
            return;
        }
        output.writeByte(1);

        switch (type) {
            case RowType rowType -> rowBlockToBinary((SqlRow) value, output, rowType);
            case ArrayType arrayType -> arrayBlockToBinary((Block) value, output, arrayType);
            case MapType mapType -> mapBlockToBinary((SqlMap) value, output, mapType);
            case DecimalType decimalType -> {
                String decimalString = decimalType.isShort()
                        ? Decimals.toString((long) value, decimalType.getScale())
                        : Decimals.toString((Int128) value, decimalType.getScale());
                writeVariableSlice(utf8Slice(decimalString), output);
            }
//            case TimeWithTimeZoneType timeType -> output.writeLong((long) value);
//            case TimestampType timestampType -> output.writeLong((long) value);
//            case TimestampWithTimeZoneType timestampType -> output.writeLong((long) value);
            default -> javaToBinarySimple(type, value, output);
        }
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    private static void javaToBinarySimple(Type type, Object value, SliceOutput output)
    {
        switch (type.getBaseName()) {
            case StandardTypes.BOOLEAN -> output.writeByte((boolean) value ? 1 : 0);
            case StandardTypes.BIGINT -> output.writeLong((long) value);
            case StandardTypes.INTEGER -> output.writeInt(toIntExact((long) value));
            case StandardTypes.SMALLINT -> output.writeShort(toIntExact((long) value));
            case StandardTypes.TINYINT -> output.writeByte(toIntExact((long) value));
            case StandardTypes.DOUBLE -> output.writeDouble((double) value);
            case StandardTypes.REAL -> output.writeInt(toIntExact((long) value));
            case StandardTypes.DATE -> output.writeInt(toIntExact((long) value));
            case StandardTypes.TIME -> output.writeLong((long) value);
            case StandardTypes.INTERVAL_YEAR_TO_MONTH -> output.writeInt(toIntExact((long) value));
            case StandardTypes.INTERVAL_DAY_TO_SECOND -> output.writeLong((long) value);
            case StandardTypes.UUID,
                 StandardTypes.IPADDRESS -> output.writeBytes((Slice) value);
            case StandardTypes.CHAR,
                 StandardTypes.VARCHAR,
                 StandardTypes.VARBINARY,
                 StandardTypes.JSON -> writeVariableSlice((Slice) value, output);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        }
    }

    private static void blockToBinary(Type type, Block block, int position, SliceOutput output)
    {
        if (block.isNull(position)) {
            output.writeByte(0);
            return;
        }
        output.writeByte(1);

        switch (type) {
//            case RowType rowType -> rowBlockToBinary((SqlRow) block.getObject(position), output, rowType);
//            case ArrayType arrayType -> arrayBlockToBinary(block.getObject(position, Block.class), output, arrayType);
//            case MapType mapType -> mapBlockToBinary((SqlMap) block.getObject(position), output, mapType);
            case BooleanType booleanType -> output.writeByte(booleanType.getBoolean(block, position) ? 1 : 0);
            case BigintType bigintType -> output.writeLong(bigintType.getLong(block, position));
            case IntegerType integerType -> output.writeInt(integerType.getInt(block, position));
            case SmallintType smallintType -> output.writeShort(smallintType.getShort(block, position));
            case TinyintType tinyintType -> output.writeByte(tinyintType.getByte(block, position));
            case DoubleType doubleType -> output.writeDouble(doubleType.getDouble(block, position));
            case RealType realType -> output.writeFloat(intBitsToFloat(realType.getInt(block, position)));
            case DecimalType decimalType -> {
                String decimalString = decimalType.isShort()
                        ? Decimals.toString(decimalType.getLong(block, position), decimalType.getScale())
                        : Decimals.toString((Int128) decimalType.getObject(block, position), decimalType.getScale());
                writeVariableSlice(utf8Slice(decimalString), output);
            }
            case DateType dateType -> output.writeInt(dateType.getInt(block, position));
            case TimeType timeType -> output.writeLong(timeType.getLong(block, position));
//            case TimestampType timestampType
//            case TimestampWithTimeZoneType timestampType

            default -> blockToBinarySimple(type, block, position, output);
        }
    }

    private static void blockToBinarySimple(Type type, Block block, int position, SliceOutput output)
    {
        switch (type.getBaseName()) {
            case StandardTypes.UUID,
                 StandardTypes.IPADDRESS -> output.writeBytes(type.getSlice(block, position));
            case StandardTypes.CHAR,
                 StandardTypes.VARCHAR,
                 StandardTypes.VARBINARY,
                 StandardTypes.JSON -> writeVariableSlice(type.getSlice(block, position), output);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        }
    }

    private static void rowBlockToBinary(SqlRow row, SliceOutput output, RowType rowType)
    {
        for (int i = 0; i < rowType.getFields().size(); i++) {
            blockToBinary(
                    rowType.getFields().get(i).getType(),
                    row.getUnderlyingFieldBlock(i),
                    row.getUnderlyingFieldPosition(i),
                    output);
        }
    }

    private static void arrayBlockToBinary(Block value, SliceOutput output, ArrayType arrayType)
    {
        ValueBlock array = value.getUnderlyingValueBlock();
        output.writeInt(array.getPositionCount());
        for (int i = 0; i < array.getPositionCount(); i++) {
            blockToBinary(arrayType.getElementType(), array, i, output);
        }
    }

    private static void mapBlockToBinary(SqlMap map, SliceOutput output, MapType mapType)
    {
        output.writeInt(map.getSize());
        for (int i = 0; i < map.getSize(); i++) {
            blockToBinary(
                    mapType.getKeyType(),
                    map.getUnderlyingKeyBlock(),
                    map.getUnderlyingKeyPosition(i),
                    output);
            blockToBinary(
                    mapType.getValueType(),
                    map.getUnderlyingValueBlock(),
                    map.getUnderlyingValuePosition(i),
                    output);
        }
    }

    public static Object binaryToJava(Type type, SliceInput input)
    {
        if (!input.readBoolean()) {
            return null;
        }

        return switch (type) {
//            case RowType rowType -> rowBinaryToJava(input, rowType);
//            case ArrayType arrayType -> arrayBinaryToJava(input, arrayType);
//            case MapType mapType -> mapBinaryToJava(input, mapType);
            case DecimalType decimalType -> {
                BigDecimal decimal = new BigDecimal(input.readSlice(input.readInt()).toStringUtf8());
                yield decimalType.isShort()
                        ? encodeShortScaledValue(decimal, decimalType.getScale(), HALF_UP)
                        : encodeScaledValue(decimal, decimalType.getScale(), HALF_UP);
            }
//            case TimeWithTimeZoneType timeType -> timeWithTimeZoneBinaryToJava(input, timeType);
//            case TimestampType timestampType -> timestampBinaryToJava(input, timestampType);
//            case TimestampWithTimeZoneType timestampType -> timestampWithTimeZoneBinaryToJava(input, timestampType);
            default -> binaryToJavaSimple(type, input);
        };
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    private static Object binaryToJavaSimple(Type type, SliceInput input)
    {
        return switch (type.getBaseName()) {
            case StandardTypes.BOOLEAN -> input.readBoolean();
            case StandardTypes.BIGINT -> input.readLong();
            case StandardTypes.INTEGER -> (long) input.readInt();
            case StandardTypes.SMALLINT -> (long) input.readShort();
            case StandardTypes.TINYINT -> (long) input.readByte();
            case StandardTypes.DOUBLE -> input.readDouble();
            case StandardTypes.REAL -> (long) input.readInt();
            case StandardTypes.DATE -> (long) input.readInt();
            case StandardTypes.TIME -> input.readLong();
            case StandardTypes.INTERVAL_YEAR_TO_MONTH -> (long) input.readInt();
            case StandardTypes.INTERVAL_DAY_TO_SECOND -> input.readLong();
            case StandardTypes.UUID,
                 StandardTypes.IPADDRESS -> input.readSlice(16);
            case StandardTypes.CHAR,
                 StandardTypes.VARCHAR,
                 StandardTypes.VARBINARY -> input.readSlice(input.readInt());
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        };
    }

    private static void writeVariableSlice(Slice value, SliceOutput output)
    {
        output.writeInt(value.length());
        output.writeBytes(value);
    }
}
