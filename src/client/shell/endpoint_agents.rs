use super::render::put_text;
use super::*;

pub(super) fn render_collapsed(
    buffer: &mut Buffer,
    area: Rect,
    endpoints: &[ClientShellEndpoint],
    active_endpoint_id: &ClientEndpointId,
    config: &ClientShellConfig,
    pinned_agents: &HashSet<preferences::ClientAgentPreferenceKey>,
    agent_manual_order: &[preferences::ClientAgentPreferenceKey],
    hits: &mut ShellHitMap,
) {
    let rows = agent_rows(
        endpoints,
        active_endpoint_id,
        config,
        pinned_agents,
        agent_manual_order,
    );
    for (index, row) in rows.into_iter().take(area.height as usize).enumerate() {
        let rect = Rect::new(area.x, area.y + index as u16, area.width, 1);
        if row.agent.focused {
            buffer.set_style(rect, Style::default().bg(config.palette.active_row_bg));
        }
        let initial = row.machine_label.chars().next().unwrap_or('?');
        put_text(
            buffer,
            rect.x,
            rect.y,
            rect.width,
            &format!(
                "{initial}{}",
                status_icon(row.agent.status, config.status_indicators)
            ),
            Style::default()
                .fg(if row.stale {
                    config.palette.overlay0
                } else {
                    status_color(row.agent.status, &config.palette)
                })
                .add_modifier(if row.stale {
                    Modifier::DIM
                } else {
                    Modifier::empty()
                }),
        );
        hits.endpoint_agents
            .push((rect, row.endpoint_id, row.agent.pane_id));
    }
}

pub(super) fn render_expanded(
    buffer: &mut Buffer,
    area: Rect,
    agent_view_label: Option<&str>,
    endpoints: &[ClientShellEndpoint],
    active_endpoint_id: &ClientEndpointId,
    config: &ClientShellConfig,
    agent_scroll: &mut usize,
    pinned_agents: &HashSet<preferences::ClientAgentPreferenceKey>,
    unread_agents: &HashSet<preferences::ClientAgentPreferenceKey>,
    agent_manual_order: &[preferences::ClientAgentPreferenceKey],
    hits: &mut ShellHitMap,
) {
    if !super::agent_sidebar::render_agent_panel_header(
        buffer,
        area,
        agent_view_label,
        config,
        hits,
    ) {
        return;
    }
    let rows = agent_rows(
        endpoints,
        active_endpoint_id,
        config,
        pinned_agents,
        agent_manual_order,
    );
    super::agent_sidebar::render_agent_list(
        buffer,
        area,
        &rows,
        agent_view_label.map(|_| " no matching agents"),
        config,
        agent_scroll,
        hits,
        |row| row.agent.rows.len(),
        |buffer, rect, row, hits| {
            super::agent_sidebar::render_agent_row(buffer, rect, &row.agent, config);
            let pin_rect = Rect::new(rect.right().saturating_sub(2), rect.y, 2.min(rect.width), 1);
            let unread_rect =
                Rect::new(rect.right().saturating_sub(4), rect.y, 2.min(rect.width), 1);
            hits.agent_pins.push((pin_rect, row.key.clone()));
            hits.agent_unreads.push((unread_rect, row.key.clone()));
            put_text(
                buffer,
                unread_rect.right().saturating_sub(1),
                unread_rect.y,
                1,
                if unread_agents.contains(&row.key) {
                    "●"
                } else {
                    "○"
                },
                Style::default().fg(if unread_agents.contains(&row.key) {
                    config.palette.accent
                } else {
                    config.palette.overlay0
                }),
            );
            put_text(
                buffer,
                pin_rect.right().saturating_sub(1),
                pin_rect.y,
                1,
                if row.pinned { "◆" } else { "◇" },
                Style::default().fg(if row.pinned {
                    config.palette.accent
                } else {
                    config.palette.overlay0
                }),
            );
            if row.stale {
                buffer.set_style(
                    rect,
                    Style::default()
                        .fg(config.palette.overlay0)
                        .add_modifier(Modifier::DIM),
                );
            }
            hits.endpoint_agents
                .push((rect, row.endpoint_id.clone(), row.agent.pane_id.clone()));
        },
    );
}

struct EndpointAgentRow {
    key: preferences::ClientAgentPreferenceKey,
    pinned: bool,
    endpoint_id: ClientEndpointId,
    machine_label: String,
    stale: bool,
    agent: super::agent_sidebar::AgentRow,
}

fn agent_rows(
    endpoints: &[ClientShellEndpoint],
    active_endpoint_id: &ClientEndpointId,
    config: &ClientShellConfig,
    pinned_agents: &HashSet<preferences::ClientAgentPreferenceKey>,
    agent_manual_order: &[preferences::ClientAgentPreferenceKey],
) -> Vec<EndpointAgentRow> {
    let mut rendered_rows = endpoints
        .iter()
        .filter_map(|endpoint| {
            endpoint.snapshot.as_deref().map(|snapshot| {
                snapshot
                    .agents
                    .iter()
                    .filter_map(|agent| {
                        super::agent_sidebar::agent_row(
                            snapshot,
                            &agent.pane_id,
                            config,
                            Some(&endpoint.label),
                        )
                    })
                    .map(|agent| ((endpoint.endpoint_id.clone(), agent.pane_id.clone()), agent))
                    .collect::<Vec<_>>()
            })
        })
        .flatten()
        .collect::<HashMap<_, _>>();

    super::aggregate_navigation::aggregate_agent_rows_with_preferences(
        endpoints,
        active_endpoint_id,
        config.agent_panel_sort,
        pinned_agents,
        agent_manual_order,
    )
    .into_iter()
    .filter_map(|row| {
        let key = (row.endpoint.endpoint_id.clone(), row.agent.pane_id.clone());
        let mut agent = rendered_rows.remove(&key)?;
        agent.focused &= row.endpoint.endpoint_id == active_endpoint_id;
        let key = preferences::ClientAgentPreferenceKey::new(
            row.endpoint.endpoint_id,
            &row.agent.pane_id,
        );
        Some(EndpointAgentRow {
            pinned: pinned_agents.contains(&key),
            key,
            endpoint_id: row.endpoint.endpoint_id.clone(),
            machine_label: row.endpoint.label.to_owned(),
            stale: row.endpoint.stale(),
            agent,
        })
    })
    .collect()
}
