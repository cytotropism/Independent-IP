SELECT  l_shipmode,
        sum(
            CASE
                WHEN o_orderpriority='1-URGENT' 
                  OR o_orderpriority='2-HIGH' 
                THEN l_quantity
                ELSE 0
            END
        ) AS high_line_count,
        sum(
            CASE
                WHEN o_orderpriority<>'1-URGENT' 
                  AND o_orderpriority<>'2-HIGH' 
                THEN l_quantity
                ELSE 0
            END
        ) AS low_line_count,
        sum(l_quantity) AS total_quantity,
        sum(l_extendedprice) AS extended_price
FROM    orders,
        lineitem
WHERE   o_orderkey=l_orderkey
AND     l_commitdate<l_receiptdate
AND     l_shipdate<l_commitdate
AND     l_receiptdate>=date '[DATE]'
AND     l_receiptdate<date '[DATE]'+INTERVAL '1' YEAR
GROUP BY
        l_shipmode
ORDER BY
        l_shipmode;
