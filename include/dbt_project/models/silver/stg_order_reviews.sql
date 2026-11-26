select
    review_id,
    order_id,
    try_to_number(review_score)                 as review_score,
    review_comment_title                        as comment_title,
    review_comment_message                      as comment_message,
    try_to_timestamp(review_creation_date)      as created_at,
    try_to_timestamp(review_answer_timestamp)   as answered_at,
    _batch_id,
    _ingested_at
from {{ source('bronze', 'order_reviews') }}
qualify row_number() over (
    partition by order_id
    order by try_to_timestamp(review_answer_timestamp) desc nulls last, _ingested_at desc
) = 1