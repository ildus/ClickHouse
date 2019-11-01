#include <numeric>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/IDataType.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionMapFill : public IFunction
{
public:
    static constexpr auto name = "mapFill";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMapFill>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception{getName() + " accepts at least two arrays for key and value", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        if (arguments.size() > 3)
            throw Exception{"too many arguments in " + getName() + " call", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const DataTypeArray * array1_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        const DataTypeArray * array2_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());

        if (!array1_type || !array2_type)
            throw Exception{getName() + " accepts two arrays for key and value", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        DataTypePtr keys_type = array1_type->getNestedType();
        WhichDataType whichKey(keys_type);

        if (!whichKey.isNativeInt() && !whichKey.isNativeUInt())
        {
            throw Exception(
                "Keys for " + getName() + " should be of native integer type (signed or unsigned)", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeTuple>(DataTypes{arguments[0], arguments[1]});
    }

    template <typename KeyType>
    bool executeInternal(
        Block & block,
        ColumnWithTypeAndName &col1,
        ColumnPtr & col2,
        ColumnPtr & max_key_column_ptr,
        const size_t result,
        const DataTypeArray * keys_type,
        const DataTypeArray * values_type)
    {
        bool max_key_is_provided = max_key_column_ptr != nullptr;
        bool max_key_column_is_const = false,
			 keys_array_is_const = false,
			 values_array_is_const = false;

        KeyType max_key = 0;

        auto keys_array = checkAndGetColumn<ColumnArray>(col1.column.get());
        if (!keys_array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(col1.column.get());
            if (!const_array)
                throw Exception("Expected array column, found " + col1.column->getName(), ErrorCodes::ILLEGAL_COLUMN);

            keys_array = checkAndGetColumn<ColumnArray>(const_array->getDataColumnPtr().get());
            keys_array_is_const = true;
        }

		DataTypeNumber<KeyType>	key_items_type;
		if (keys_type->getNestedType()->getTypeId() != key_items_type.getTypeId())
			return false;

		auto values_array = checkAndGetColumn<ColumnArray>(col2.get());
        if (!values_array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(col2.get());
            if (!const_array)
                throw Exception("Expected array column, found " + col2->getName(), ErrorCodes::ILLEGAL_COLUMN);

            values_array = checkAndGetColumn<ColumnArray>(const_array->getDataColumnPtr().get());
            values_array_is_const = true;
        }

        if (!keys_array || !values_array)
            /* something went wrong */
            throw Exception{"Illegal columns in arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};

        /* Arrays could have different sizes if one of them is constant (therefore it has size 1) */
        if (!keys_array_is_const && !values_array_is_const && (keys_array->size() != values_array->size()))
            throw Exception{"Arrays for " + getName() + " should have equal size of rows", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (max_key_is_provided && isColumnConst(*max_key_column_ptr))
        {
            auto * column_const = static_cast<const ColumnConst *>(&*max_key_column_ptr);
            max_key = column_const->template getValue<KeyType>();
            max_key_column_is_const = true;
        }

        // Original offsets
        const IColumn::Offsets & keys_offset = keys_array->getOffsets();
        const IColumn::Offsets & values_offset = values_array->getOffsets();

        // Create result vectors and offsets for both of them
        auto keys_vector = ColumnVector<KeyType>::create();
        auto values_vector = values_type->getNestedType()->createColumn();

        typename ColumnVector<KeyType>::Container & keys = keys_vector->getData();

        auto offsets_vector = ColumnArray::ColumnOffsets::create(keys_array->size());
        typename ColumnVector<IColumn::Offset>::Container & offsets = offsets_vector->getData();

        size_t rows = keys_array_is_const ? values_array->size() : keys_array->size();

        /* Prepare offsets array so we can just use indexes on it */
        offsets.resize(rows);

        IColumn::Offset offset{0}, prev_keys_offset{0}, prev_values_offset{0};

        /*
         * Iterate through two arrays and fill result values. It is possible that one or
         * both arrays are const, so we make sure we don't increase row index for them.
         */
        for (size_t row = 0, row1 = 0, row2 = 0; row < rows; ++row)
        {
            KeyType prev_key{};
            size_t array_size = keys_offset[row1] - prev_keys_offset;

            /* update the current max key if it's not constant */
            if (max_key_is_provided && !max_key_column_is_const)
            {
                Field value;
                (*max_key_column_ptr).get(row, value);
                max_key = value.get<KeyType>();
            }

            if (values_offset[row2] - prev_values_offset != array_size)
                throw Exception{"Arrays for " + getName() + " should have equal size of elements", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            Field value;
            bool done = false;

            for (size_t i = 0; !done && i < array_size; ++i)
            {
                auto key = static_cast<KeyType>(keys_array->getData().getInt(prev_keys_offset + i));

                /*
                 * if there is gap, fill by generated keys and default value,
                 * prev_key at first is not initialized, so start when i > 0
                 */
                for (; i > 0 && !done && prev_key < key; ++prev_key)
                {
                    keys.push_back(prev_key);
                    values_vector->insertDefault();
                    ++offset;

                    done = max_key_is_provided && prev_key >= max_key;
                }

                if (!done)
                {
                    values_array->getData().get(prev_values_offset + i, value);
                    keys.push_back(key);
                    values_vector->insert(value);
                    prev_key = key + 1;
                    ++offset;

                    done = max_key_is_provided && key >= max_key;
                }
            }

            /* if max key if provided, try to extend the current arrays */
            if (max_key_is_provided)
            {
                while (prev_key <= max_key)
                {
					KeyType	old = prev_key;
                    keys.push_back(prev_key);
                    values_vector->insertDefault();
					++prev_key;
					++offset;

					/* check for overflow */
					if (prev_key < old)
						break;
                }
            }

            if (!keys_array_is_const)
            {
                prev_keys_offset = keys_offset[row1];
                ++row1;
            }
            if (!values_array_is_const)
            {
                prev_values_offset = values_offset[row2];
                ++row2;
            }

            offsets[row] = offset;
        }

        auto result_keys_array = ColumnArray::create(std::move(keys_vector), std::move(offsets_vector->clone()));
        auto result_values_array = ColumnArray::create(std::move(values_vector), std::move(offsets_vector));

        ColumnPtr res = ColumnTuple::create(Columns{std::move(result_keys_array), std::move(result_values_array)});
        if (keys_array_is_const && values_array_is_const)
            res = ColumnConst::create(res, 1);

        block.getByPosition(result).column = res;
        return true;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        auto col1 = block.safeGetByPosition(arguments[0]),
			 col2 = block.safeGetByPosition(arguments[1]);

        const DataTypeArray * k = checkAndGetDataType<DataTypeArray>(col1.type.get());
        const DataTypeArray * v = checkAndGetDataType<DataTypeArray>(col2.type.get());
        WhichDataType whichVal(v->getNestedType());

        ColumnPtr max_key_column_ptr = nullptr;

        if (arguments.size() == 3)
        {
            /* max key provided */
            auto col3 = block.safeGetByPosition(arguments[2]);

            WhichDataType whichMaxKey(col3.type.get());
            if (whichMaxKey.isNullable())
                throw Exception(
                    "Max key argument in arguments of function " + getName() + " can not be Nullable",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			if (k->getNestedType()->getTypeId() != col3.type->getTypeId())
                throw Exception(
                    "Max key argument of function " + getName() + " should be same type as keys",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            max_key_column_ptr = block.getByPosition(arguments[2]).column;
        }

        /* Determine array types and call according functions */
        if (!executeInternal<Int8>(block, col1, col2.column, max_key_column_ptr, result, k, v)
            && !executeInternal<Int16>(block, col1, col2.column, max_key_column_ptr, result, k, v)
            && !executeInternal<Int32>(block, col1, col2.column, max_key_column_ptr, result, k, v)
            && !executeInternal<Int64>(block, col1, col2.column, max_key_column_ptr, result, k, v)
            && !executeInternal<UInt8>(block, col1, col2.column, max_key_column_ptr, result, k, v)
            && !executeInternal<UInt16>(block, col1, col2.column, max_key_column_ptr, result, k, v)
            && !executeInternal<UInt32>(block, col1, col2.column, max_key_column_ptr, result, k, v)
            && !executeInternal<UInt64>(block, col1, col2.column, max_key_column_ptr, result, k, v))
        {
            throw Exception{"Illegal columns in arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }
    }
};

void registerFunctionMapFill(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMapFill>();
}

}
