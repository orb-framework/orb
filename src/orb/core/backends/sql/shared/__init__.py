# __author__ = 'ehulser'
#
#  SELECT sessions.id AS session_id,
#     session_statuses.name AS status_name,
#     vehicles.vin,
#     sessions.description,
#     sessions.updated_at,
#     comments_user.username AS last_commented_by_username,
#     comments_user.display_name AS last_commented_by_name,
#     escalations_assigned_to.username AS assigned_to_username,
#     escalations_assigned_to.display_name AS assigned_to_name,
#     count(escalations.*) AS escalation_count,
#     service_centers.id AS service_center_id,
#     service_centers.name AS service_center_name,
#     regions.id AS region_id,
#     regions.name AS region_name
#    FROM sessions
#      LEFT JOIN session_statuses AS sessions_statuses_status ON sessions_statuses_status.id = sessions.status_id
#      LEFT JOIN snapshots ON snapshots.id = sessions.snapshot_id
#      LEFT JOIN service_centers ON service_centers.id = snapshots.service_center_id
#      LEFT JOIN regions ON regions.id = service_centers.region_id
#      LEFT JOIN vehicles ON vehicles.id = snapshots.vehicle_id
#      LEFT JOIN escalations ON escalations.session_id = sessions.id
#      LEFT JOIN users escalations_assigned_to ON escalations.assigned_to_id = escalations_assigned_to.id
#      LEFT JOIN session_comments ON session_comments.id = sessions.id
#      LEFT JOIN comments ON comments.id = session_comments.comment_id
#      LEFT JOIN users comments_user ON comments.user_id = comments_user.id
#   GROUP BY sessions.id, session_statuses.name, vehicles.vin, snapshots.service_center_id, service_centers.id, service_centers.name, regions.id, regions.name, comments_user.username, comments_user.display_name, escalations_assigned_to.username, escalations_assigned_to.display_name;
