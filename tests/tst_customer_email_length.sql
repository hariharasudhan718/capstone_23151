select * from {{ ref('customer_transformations') }} c where length(c.email) > 2*(
    select top 1 length(c.email) from {{ ref('customer_transformations') }} c order by length(c.email) desc 
)