#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextEditor {
    text: String,
    cursor: usize,
}

impl TextEditor {
    pub fn new(text: impl Into<String>) -> Self {
        let text = text.into();
        let cursor = text.len();
        Self { text, cursor }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    pub fn set_text(&mut self, text: impl Into<String>) {
        self.text = text.into();
        self.cursor = self.text.len();
    }

    pub fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
    }

    pub fn insert_char(&mut self, ch: char) {
        self.text.insert(self.cursor, ch);
        self.cursor += ch.len_utf8();
    }

    pub fn insert_str(&mut self, value: &str) {
        self.text.insert_str(self.cursor, value);
        self.cursor += value.len();
    }

    pub fn insert_newline(&mut self) {
        self.insert_char('\n');
    }

    pub fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let previous = self.text[..self.cursor]
            .chars()
            .next_back()
            .expect("cursor > 0 must have previous character");
        let start = self.cursor - previous.len_utf8();
        self.text.drain(start..self.cursor);
        self.cursor = start;
    }

    pub fn delete(&mut self) {
        if self.cursor >= self.text.len() {
            return;
        }
        let next = self.text[self.cursor..]
            .chars()
            .next()
            .expect("cursor in bounds must have next character");
        self.text.drain(self.cursor..self.cursor + next.len_utf8());
    }

    pub fn move_left(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let previous = self.text[..self.cursor]
            .chars()
            .next_back()
            .expect("cursor > 0 must have previous character");
        self.cursor -= previous.len_utf8();
    }

    pub fn move_right(&mut self) {
        if self.cursor >= self.text.len() {
            return;
        }
        let next = self.text[self.cursor..]
            .chars()
            .next()
            .expect("cursor in bounds must have next character");
        self.cursor += next.len_utf8();
    }

    pub fn move_home(&mut self) {
        self.cursor = current_line_start(&self.text, self.cursor);
    }

    pub fn move_end(&mut self) {
        self.cursor = current_line_end(&self.text, self.cursor);
    }

    pub fn move_up(&mut self) {
        let current_start = current_line_start(&self.text, self.cursor);
        if current_start == 0 {
            return;
        }
        let column = self.cursor - current_start;
        let previous_end = current_start.saturating_sub(1);
        let previous_start = current_line_start(&self.text, previous_end);
        let previous_len = previous_end - previous_start;
        self.cursor = previous_start + column.min(previous_len);
    }

    pub fn move_down(&mut self) {
        let current_start = current_line_start(&self.text, self.cursor);
        let current_end = current_line_end(&self.text, self.cursor);
        if current_end >= self.text.len() {
            return;
        }
        let next_start = current_end + 1;
        let next_end = current_line_end(&self.text, next_start);
        let column = self.cursor - current_start;
        self.cursor = next_start + column.min(next_end - next_start);
    }

    pub fn cursor_line_col(&self) -> (usize, usize) {
        let line = self.text[..self.cursor]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count();
        let line_start = current_line_start(&self.text, self.cursor);
        (line, self.cursor - line_start)
    }
}

fn current_line_start(text: &str, cursor: usize) -> usize {
    match text[..cursor].rfind('\n') {
        Some(index) => index + 1,
        None => 0,
    }
}

fn current_line_end(text: &str, cursor: usize) -> usize {
    match text[cursor..].find('\n') {
        Some(offset) => cursor + offset,
        None => text.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::TextEditor;

    #[test]
    fn backspace_removes_previous_character() {
        let mut editor = TextEditor::new("ab");
        editor.backspace();
        assert_eq!(editor.text(), "a");
    }

    #[test]
    fn delete_removes_character_at_cursor() {
        let mut editor = TextEditor::new("abc");
        editor.move_left();
        editor.move_left();
        editor.delete();
        assert_eq!(editor.text(), "ac");
    }

    #[test]
    fn move_up_and_down_preserve_column_when_possible() {
        let mut editor = TextEditor::new("abc\ndefg");
        editor.move_home();
        editor.move_down();
        editor.move_right();
        editor.move_right();
        let (line, col) = editor.cursor_line_col();
        assert_eq!((line, col), (1, 2));
        editor.move_up();
        let (line, col) = editor.cursor_line_col();
        assert_eq!((line, col), (0, 2));
    }
}
