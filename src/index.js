const { parseQuery } = require('./queryParser');
const readCSV = require('./csvReader');

// Helper functions for different JOIN types
async function performInnerJoin(data, joinData, joinCondition, fields, table) {
    const newData = data.flatMap((mainRow) => {
        return joinData
            .filter((joinRow) => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
            .map((joinRow) => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] =
                        tableName === table
                            ? mainRow[fieldName]
                            : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
    return newData;
}

function createResultRow(
    mainRow,
    joinRow,
    fields,
    table,
    includeAllMainFields
) {
    const resultRow = {};

    if (includeAllMainFields) {
        // Include all fields from the main table
        Object.keys(mainRow || {}).forEach((key) => {
            const prefixedKey = `${table}.${key}`;
            resultRow[prefixedKey] = mainRow ? mainRow[key] : null;
        });
    }

    // Now, add or overwrite with the fields specified in the query
    fields.forEach((field) => {
        const [tableName, fieldName] = field.includes('.')
            ? field.split('.')
            : [table, field];
        resultRow[field] =
            tableName === table && mainRow
                ? mainRow[fieldName]
                : joinRow
                ? joinRow[fieldName]
                : null;
    });

    return resultRow;
}

function getValueFromRow(row, compoundFieldName) {
    const [tableName, fieldName] = compoundFieldName.split('.');
    return row[`${tableName}.${fieldName}`] || row[fieldName];
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
    const mainTableRowStructure =
        data.length > 0
            ? Object.keys(data[0]).reduce((acc, key) => {
                  acc[key] = null;
                  return acc;
              }, {})
            : {};

    return joinData.map((joinRow) => {
        const mainRowMatch = data.find((mainRow) => {
            const mainValue = getValueFromRow(mainRow, joinCondition.left);
            const joinValue = getValueFromRow(joinRow, joinCondition.right);
            return mainValue === joinValue;
        });

        const mainRowToUse = mainRowMatch || mainTableRowStructure;

        return createResultRow(mainRowToUse, joinRow, fields, table, true);
    });
}
async function performLeftJoin(data, joinData, joinCondition, fields, table) {
    console.log({ data, joinData, joinCondition, fields, table });
    const newdata = data.flatMap((mainRow) => {
        const matches = joinData.filter((joinRow) => {
            const mainValue = getValueFromRow(mainRow, joinCondition.left);
            const joinValue = getValueFromRow(joinRow, joinCondition.right);
            return mainValue === joinValue;
        });

        if (matches.length === 0) {
            return [createResultRow(mainRow, null, fields, table, true)];
        }

        return matches.map((joinRow) =>
            createResultRow(mainRow, joinRow, fields, table, true)
        );
    });
    return newdata;
}

async function executeSELECTQuery(query) {
    // Now we will have joinTable, joinCondition in the parsed query
    const { fields, table, whereClauses, joinTable, joinCondition, joinType } =
        parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Logic for applying JOINs
    if (joinTable && joinCondition) {
        console.log(joinTable);
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = await performInnerJoin(
                    data,
                    joinData,
                    joinCondition,
                    fields,
                    table
                );
                break;
            case 'LEFT':
                data = await performLeftJoin(
                    data,
                    joinData,
                    joinCondition,
                    fields,
                    table
                );
                break;
            case 'RIGHT':
                data = await performRightJoin(
                    data,
                    joinData,
                    joinCondition,
                    fields,
                    table
                );
                break;
            default:
                throw new Error('Invalid JOIN type');
            // Handle default case or unsupported JOIN types
        }
    }

    // Apply WHERE clause filtering after JOIN (or on the original data if no join)
    const filteredData =
        whereClauses.length > 0
            ? data.filter((row) =>
                  whereClauses.every((clause) => evaluateCondition(row, clause))
              )
            : data;

    // Select fields
    const result = filteredData.map((row) => {
        const selectedRow = {};
        fields.forEach((field) => {
            // Assuming 'field' is just the column name without table prefix
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });

    return result;
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=':
            return row[field] === value;
        case '!=':
            return row[field] !== value;
        case '>':
            return row[field] > value;
        case '<':
            return row[field] < value;
        case '>=':
            return row[field] >= value;
        case '<=':
            return row[field] <= value;
        default:
            throw new Error(`Unsupported operator: ${operator}`);
    }
}

module.exports = executeSELECTQuery;
