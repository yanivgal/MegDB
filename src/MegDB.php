<?php

namespace yanivgal;

class MegDB
{
    /**
     * @var string
     */
    private $dns;

    /**
     * @var string
     */
    private $username;

    /**
     * @var string
     */
    private $password;

    /**
     * @var array
     */
    private $options;

    /**
     * @var \PDO
     */
    private $pdo;

    /**
     * @var \PDOStatement
     */
    private $statement;

    public function __construct($driver, $host, $dbName, $username, $password)
    {
        $this->dns = $driver . ':host=' . $host . ';dbname=' . $dbName;
        $this->username = $username;
        $this->password = $password;

        $this->options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_TIMEOUT => 30
        ];
    }

    /**
     * Executes raw SQL statement
     *
     * @param string $statement
     * @return MegDB
     */
    public function exec($statement)
    {
        $db = $this->connect();

        $db->exec($statement);

        return $this;
    }

    /**
     * Executes an SQL statement created with raw query string from $queryString
     * and with bound values from $values array
     *
     * @param string $queryString
     * @param array $values
     * @return MegDB|mixed
     */
    public function query($queryString, $values = [])
    {
        $db = $this->connect();

        $this->statement = $db->prepare($queryString);
        $this->bindValues($values);
        $this->statement->execute();

        return $this;
    }

    /**
     * Builds a Select query using passed variables
     *
     * @param string $table
     * @param array $columns
     * @param array $where
     * @param string $groupBy
     * @param string $having
     * @param string $orderBy
     * @param string $limit
     * @param bool $distinct
     * @return MegDB
     */
    public function select(
        $table,
        $columns,
        $where,
        $groupBy = null,
        $having = null,
        $orderBy = null,
        $limit = null,
        $distinct = false
    ) {
        $db = $this->connect();

        $queryString = 'SELECT ';

        if ($distinct) {
            $queryString .= 'DISTINCT ';
        }

        $queryString .= $this->toCommaString($columns) . ' ';

        $queryString .= 'FROM ' . $table . ' ' . $this->toWhereClause($where);

        if (isset($groupBy)) {
            $queryString .= ' GROUP BY ' . $groupBy;
        }

        if (isset($having)) {
            $queryString .= ' HAVING ' . $having;
        }

        if (isset($orderBy)) {
            $queryString .= ' ORDER BY ' . $orderBy;
        }

        if (isset($limit)) {
            $queryString .= ' LIMIT ' . $limit;
        }

        $this->statement = $db->prepare($queryString);
        $this->bindValues($where);
        $this->statement->execute();

        return $this;
    }

    /**
     * Builds and executes an Insert query using passed variables
     *
     * @param string $table
     * @param array $values [col1 => val1, col2 => val2, ... ]
     * @return int last insert id
     */
    public function insert($table, $values)
    {
        $db = $this->connect();

        $columns = $this->toCommaString(array_keys($values));

        $v = $this->toCommaString($this->valuesToQuestionMark($values));

        $queryString = "INSERT INTO {$table}
                                    ($columns)
                        VALUES      ({$v})";

        $this->statement = $db->prepare($queryString);
        $this->bindValues($values);
        $this->statement->execute();

        return $db->lastInsertId();
    }

    /**
     * Builds and executes an Update query using passed variables
     *
     * @param string $table
     * @param array $values
     * @param array $where
     * @return int number of effected rows
     */
    public function update($table, $values, $where)
    {
        $db = $this->connect();

        $queryString = 'UPDATE ' . $table. ' '
            . $this->toSetClause($values) . ' '
            . $this->toWhereClause($where);

        $this->statement = $db->prepare($queryString);
        $this->bindValues(array_merge($values, $where));
        $this->statement->execute();

        return $this->statement->rowCount();
    }

    /**
     * Builds and executes a Delete query using passed variables
     *
     * @param string $table
     * @param array $where
     * @return int number of effected rows
     */
    public function delete($table, $where)
    {
        $db = $this->connect();

        $whereClause = $this->toWhereClause($where);

        $queryString = "DELETE FROM {$table} {$whereClause}";

        $this->statement = $db->prepare($queryString);
        $this->bindValues($where);
        $this->statement->execute();

        return $this->statement->rowCount();
    }

    /**
     * Builds and executes an Insert On Duplicate Update query
     * using passed variables
     *
     * @param string $table
     * @param array $values [col1 => val1, col2 => val2, ... ]
     * @return int last insert id
     */
    public function insertOnDuplicateUpdate($table, $values)
    {
        $db = $this->connect();

        $columns = $this->toCommaString(array_keys($values));

        $v = $this->toCommaString($this->valuesToQuestionMark($values));
        $onDuplicateUpdateClause = $this->toOnDuplicateUpdateClause($values);

        $queryString = "INSERT INTO {$table}
                        ({$columns})
                        VALUES ({$v})
                        {$onDuplicateUpdateClause}";

        $this->statement = $db->prepare($queryString);
        $this->bindValues(array_merge($values, array_values($values)));
        $this->statement->execute();

        return $db->lastInsertId();
    }

    /**
     * Fetches the db statement and returns first row as an associative array
     * which 'key' is the column name and 'value' is the column value.<p>
     * This method works best with a single row result.
     *
     * @return array
     */
    public function fetchAssoc()
    {
        $res = $this->statement->fetch();
        if (!empty($res)) {
            foreach ($res as $name => $val) {
                if (is_numeric($name)) {
                    unset($res[$name]);
                }
            }
        }
        return $res;
    }

    /**
     * Fetches the db statement and returns all rows as an array
     *
     * @return array
     */
    public function fetchAll()
    {
        $res = $this->statement->fetchAll();
        foreach ($res as $rowName => $row) {
            foreach ($row as $col => $val) {
                if (is_numeric($col)) {
                    unset($res[$rowName][$col]);
                }
            }
        }
        return $res;
    }

    /**
     * @return array
     */
    public function fetchAllName()
    {
        return $this->statement->fetchAll(\PDO::FETCH_ASSOC);
    }

    /**
     * @return array
     */
    public function fetchAllNum()
    {
        return $this->statement->fetchAll(\PDO::FETCH_NUM);
    }

    /**
     * @return array
     */
    public function fetchNum()
    {
        return $this->statement->fetch(\PDO::FETCH_NUM);
    }

    /**
     * Fetches the db statement and returns all rows as an associative array
     * which each 'key' is the value of the first column of the respected row
     *
     * @return array
     */
    public function fetchAllAssoc()
    {
        $res = $this->statement->fetchAll();
        foreach ($res as $rowName => $row) {
            $firstVal = '';
            foreach ($row as $col => $val) {
                if ($val = $row[0]) {
                    $firstVal = $val;
                }
                if (is_numeric($col)) {
                    unset($res[$rowName][$col]);
                }
            }
            $res[$firstVal] = $res[$rowName];
            unset($res[$rowName]);
        }
        return $res;
    }

    /**
     * Fetches the db statement and returns all first columns' rows as an array
     *
     * @return array
     */
    public function fetchAllValue()
    {
        return $this->statement->fetchAll(\PDO::FETCH_COLUMN);
    }

    /**
     * Fetches the db statement and returns one value which is the first row,
     * first column cell value
     *
     * @return string
     */
    public function fetchValue()
    {
        return $this->statement->fetchColumn();
    }

    /**
     * Returns the prepared query string
     *
     * @return string
     */
    public function getQueryString()
    {
        return $this->statement->queryString;
    }

    /**
     * Connect to database
     *
     * @return \PDO
     */
    private function connect()
    {
        // Don't connect twice
        if ($this->pdo) {
            return $this->pdo;
        }

        $this->pdo = new \PDO(
            $this->dns,
            $this->username,
            $this->password,
            $this->options
        );

        return $this->pdo;
    }

    /**
     * @return bool
     */
    public function isConnected()
    {
        return isset($this->pdo);
    }

    public function disconnect()
    {
        $this->pdo = null;
    }

    /**
     * Converts values array into string with separating commas.
     * [key1 => val1, key2 => val2, key3 => val3]
     * into
     * 'val1,val2,val3'
     *
     * @param $array
     * @return string
     */
    private function toCommaString($array)
    {
        return implode(',', $array);
    }

    /**
     * Converts a $where array into SQL WHERE clause
     *
     * @param array $where
     * @return string
     */
    private function toWhereClause($where)
    {
        return $this->createQueryClause($where, 'WHERE');
    }

    /**
     * Converts a $set array into SQL SET clause
     *
     * @param array $set
     * @return string
     */
    private function toSetClause($set)
    {
        return $this->createQueryClause($set, 'SET');
    }

    /**
     * Converts a $onDuplicateKeyUpdate array
     * into SQL ON DUPLICATE KEY UPDATE clause
     *
     * @param array $onDuplicateKeyUpdate
     * @return string
     */
    private function toOnDuplicateUpdateClause($onDuplicateKeyUpdate)
    {
        return $this->createQueryClause(
            $onDuplicateKeyUpdate,
            'ON DUPLICATE KEY UPDATE'
        );
    }

    /**
     * Converts array into SQL clause per $clauseType
     *
     * @param array $array
     * @param string $clauseType WHERE | SET | ON DUPLICATE KEY UPDATE
     * @return string
     */
    private function createQueryClause($array, $clauseType)
    {
        if (empty($array)) {
            return '';
        }
        switch ($clauseType) {
            case 'WHERE':
                $divider = ' AND ';
                break;
            case 'SET':
                $divider = ', ';
                break;
            case 'ON DUPLICATE KEY UPDATE':
                $divider = ', ';
                break;
            default:
                return '';
        }
        $clause = $clauseType . ' ';
        foreach ($array as $col => $value) {
            $clause .= $col . ' = ?' . $divider;
        }
        $clause = substr($clause, 0, -1 * strlen($divider));
        return $clause;
    }

    /**
     * Converts all values in given array to '?'
     *
     * @param array $array
     * @return array
     */
    private function valuesToQuestionMark($array)
    {
        return array_map(function() { return '?'; }, $array);
    }

    /**
     * Binds values to statement
     *
     * @param $array
     */
    private function bindValues($array)
    {
        $array = array_values($array);
        foreach ($array as $key => $value) {
            $this->statement->bindValue($key + 1, $value);
        }
    }
}