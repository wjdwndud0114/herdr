use super::{App, AppState};

impl AppState {
    pub(crate) fn update_host_terminal_theme_state(
        &mut self,
        kind: crate::terminal_theme::DefaultColorKind,
        color: crate::terminal_theme::RgbColor,
    ) -> bool {
        let mut changed = false;
        if matches!(kind, crate::terminal_theme::DefaultColorKind::Background)
            && !self.host_terminal_appearance_explicit
        {
            changed |= self.set_host_terminal_appearance_for_presentation(
                Some(color.inferred_appearance()),
                false,
            );
        }
        changed
            | self.set_host_terminal_theme_for_presentation(
                self.host_terminal_theme.with_color(kind, color),
            )
    }

    pub(crate) fn update_host_terminal_palette_state(
        &mut self,
        colors: &[(u8, crate::terminal_theme::RgbColor)],
    ) -> bool {
        let mut next_theme = self.host_terminal_theme;
        for &(index, color) in colors {
            next_theme = next_theme.with_palette_color(index, color);
        }
        self.set_host_terminal_theme_for_presentation(next_theme)
    }

    pub(crate) fn set_host_terminal_appearance_for_presentation(
        &mut self,
        appearance: Option<crate::terminal_theme::HostAppearance>,
        explicit: bool,
    ) -> bool {
        if self.host_terminal_appearance == appearance
            && self.host_terminal_appearance_explicit == explicit
        {
            return false;
        }
        self.host_terminal_appearance = appearance;
        self.host_terminal_appearance_explicit = explicit;
        self.refresh_effective_theme_for_presentation()
    }

    pub(crate) fn set_host_terminal_theme_for_presentation(
        &mut self,
        theme: crate::terminal_theme::TerminalTheme,
    ) -> bool {
        if theme == self.host_terminal_theme {
            return false;
        }
        self.host_terminal_theme = theme;
        true
    }

    fn refresh_effective_theme_for_presentation(&mut self) -> bool {
        let (palette, theme_name) =
            super::resolve_effective_theme(&self.theme_runtime, self.host_terminal_appearance);
        if self.theme_name == theme_name && self.palette == palette {
            return false;
        }
        self.theme_name = theme_name;
        self.palette = palette;
        true
    }
}

impl App {
    #[cfg(not(windows))]
    pub(super) fn query_host_terminal_appearance(&self) {
        use std::io::Write;

        let _ = std::io::stdout()
            .write_all(crate::terminal_theme::HOST_COLOR_SCHEME_QUERY_SEQUENCE.as_bytes());
        let _ = std::io::stdout().flush();
    }

    pub(super) fn query_host_terminal_theme(&self) {
        use std::io::Write;

        let query = crate::terminal_theme::host_terminal_theme_query_sequence();
        let _ = std::io::stdout().write_all(query.as_bytes());
        let _ = std::io::stdout().flush();
    }

    pub(super) fn update_host_terminal_theme(
        &mut self,
        kind: crate::terminal_theme::DefaultColorKind,
        color: crate::terminal_theme::RgbColor,
    ) -> bool {
        let changed = self.state.update_host_terminal_theme_state(kind, color);
        if changed {
            self.apply_host_terminal_appearance_to_panes();
            self.apply_host_terminal_theme_to_panes();
        }
        changed
    }

    pub(super) fn update_host_terminal_palette_colors(
        &mut self,
        colors: &[(u8, crate::terminal_theme::RgbColor)],
    ) -> bool {
        let changed = self.state.update_host_terminal_palette_state(colors);
        if changed {
            self.apply_host_terminal_theme_to_panes();
        }
        changed
    }

    pub(super) fn set_host_terminal_appearance(
        &mut self,
        appearance: crate::terminal_theme::HostAppearance,
        explicit: bool,
    ) -> bool {
        if self.state.host_terminal_appearance_explicit && !explicit {
            return false;
        }
        let changed = self
            .state
            .set_host_terminal_appearance_for_presentation(Some(appearance), explicit);
        if changed {
            self.apply_host_terminal_appearance_to_panes();
            self.render_dirty.request_generic();
            self.render_notify.notify_one();
        }
        changed
    }

    pub(crate) fn set_host_terminal_appearance_state(
        &mut self,
        appearance: Option<crate::terminal_theme::HostAppearance>,
        explicit: bool,
    ) -> bool {
        let changed = self
            .state
            .set_host_terminal_appearance_for_presentation(appearance, explicit);
        if changed {
            self.apply_host_terminal_appearance_to_panes();
            self.render_dirty.request_generic();
            self.render_notify.notify_one();
        }
        changed
    }

    pub(crate) fn set_host_terminal_theme(
        &mut self,
        theme: crate::terminal_theme::TerminalTheme,
    ) -> bool {
        let changed = self.state.set_host_terminal_theme_for_presentation(theme);
        if changed {
            self.apply_host_terminal_theme_to_panes();
        }
        changed
    }

    pub(super) fn refresh_effective_app_theme(&mut self) -> bool {
        let changed = self.state.refresh_effective_theme_for_presentation();
        if changed {
            self.render_dirty.request_generic();
            self.render_notify.notify_one();
        }
        changed
    }

    fn apply_host_terminal_appearance_to_panes(&self) {
        for runtime in self.terminal_runtimes.values() {
            runtime.apply_host_terminal_appearance(self.state.host_terminal_appearance);
        }
    }

    fn apply_host_terminal_theme_to_panes(&self) {
        for runtime in self.terminal_runtimes.values() {
            runtime.apply_host_terminal_theme(self.state.host_terminal_theme);
        }

        self.render_dirty.request_generic();
        self.render_notify.notify_one();
    }
}
