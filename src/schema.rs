table! {
    active_lora_transmitters (worker_id) {
        worker_id -> Int4,
    }
}

table! {
    active_wifi_transmitters (worker_id) {
        worker_id -> Int4,
    }
}

table! {
    worker_destinations (worker_id) {
        worker_id -> Int4,
        dest_x -> Float4,
        dest_y -> Float4,
    }
}

table! {
    worker_positions (worker_id) {
        worker_id -> Int4,
        x -> Float4,
        y -> Float4,
    }
}

table! {
    worker_velocities (worker_id) {
        worker_id -> Int4,
        vel_x -> Float4,
        vel_y -> Float4,
    }
}

table! {
    workers (id) {
        id -> Int4,
        worker_name -> Varchar,
        worker_id -> Varchar,
        short_range_address -> Nullable<Varchar>,
        long_range_address -> Nullable<Varchar>,
    }
}

joinable!(active_lora_transmitters -> workers (worker_id));
joinable!(active_wifi_transmitters -> workers (worker_id));
joinable!(worker_destinations -> workers (worker_id));
joinable!(worker_positions -> workers (worker_id));
joinable!(worker_velocities -> workers (worker_id));

allow_tables_to_appear_in_same_query!(
    active_lora_transmitters,
    active_wifi_transmitters,
    worker_destinations,
    worker_positions,
    worker_velocities,
    workers,
);
