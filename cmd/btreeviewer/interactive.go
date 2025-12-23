package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/index"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FAFAFA")).
			Background(lipgloss.Color("#7D56F4")).
			Padding(0, 1).
			MarginBottom(1)

	focusedStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true)
	subtleStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	leafStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	internalStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("33"))
	rangeStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("229")).Italic(true)
)

type model struct {
	v           *viewer
	indexName   string
	tableName   string
	leafLayout  *record.Layout
	dirLayout   *record.Layout
	tableLayout *record.Layout

	root       *node
	cursorItem int
	items      []navItem

	verificationResult string

	width  int
	height int
	vp     viewport.Model
	err    error
}

type navItem struct {
	node       *node
	entry      *entry
	entryIndex int
}

type node struct {
	parent   *node
	children []*node
	expanded bool

	filename string
	blkNum   int
	isLeaf   bool
	layout   *record.Layout

	flag    int
	numRecs int
	entries []entry
	loaded  bool
	level   int
}

type entry struct {
	val      any
	isChild  bool
	childBlk int
	rid      record.RID
}

func (v *viewer) runInteractive(name string) {
	ii, leafLayout, dirLayout, tableName, err := v.resolveIndex(name)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fieldName := ii.FieldName()
	_ = fieldName // Might use later in title

	rootBlk := 0
	isRootLeaf := false
	dirSize, _ := v.tx.Size(name + "dir")
	if dirSize == 0 {
		isRootLeaf = true
	}

	root := &node{
		filename: name + "dir",
		blkNum:   rootBlk,
		isLeaf:   isRootLeaf,
		layout:   dirLayout,
		level:    0,
	}
	if isRootLeaf {
		root.filename = name + "leaf"
		root.layout = leafLayout
	}

	// Load table layout for verification
	tm_meta := metadata.NewTableManager(false, v.tx)
	tableLayout, _ := tm_meta.GetLayout(tableName, v.tx)

	m := model{
		v:           v,
		indexName:   name,
		tableName:   tableName,
		leafLayout:  leafLayout,
		dirLayout:   dirLayout,
		tableLayout: tableLayout,
		root:        root,
	}

	m.loadNode(root)
	root.expanded = true
	m.updateFlattened()
	m.cursorItem = 0

	// Initialize viewport
	m.vp = viewport.New(0, 0)
	m.vp.SetContent(m.renderItems())

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error running program: %v\n", err)
		os.Exit(1)
	}
}

func (m *model) loadNode(n *node) {
	if n.loaded {
		return
	}
	blk := file.NewBlockID(n.filename, n.blkNum)
	page, err := index.NewBTPage(m.v.tx, blk, n.layout)
	if err != nil {
		return
	}
	defer page.Close()

	n.flag, _ = page.GetFlag()
	n.numRecs, _ = page.GetNumRecs()
	n.entries = make([]entry, 0, n.numRecs)

	for i := 0; i < n.numRecs; i++ {
		val, _ := page.GetVal(i, "dataval")
		if n.isLeaf {
			rid, _ := page.GetDataRid(i)
			n.entries = append(n.entries, entry{val: val, rid: *rid})
		} else {
			childBlk, _ := page.GetInt(i, "block")
			childIsLeaf := (n.flag == 1)
			n.entries = append(n.entries, entry{val: val, isChild: true, childBlk: childBlk})

			child := &node{
				parent:   n,
				filename: m.v.getChildTbl(n.filename, childIsLeaf),
				blkNum:   childBlk,
				isLeaf:   childIsLeaf,
				layout:   m.dirLayout,
				level:    n.level + 1,
			}
			if childIsLeaf {
				child.layout = m.leafLayout
			}
			m.loadHeader(child)
			n.children = append(n.children, child)
		}
	}
	n.loaded = true
}

func (m *model) loadHeader(n *node) {
	blk := file.NewBlockID(n.filename, n.blkNum)
	page, err := index.NewBTPage(m.v.tx, blk, n.layout)
	if err != nil {
		return
	}
	defer page.Close()

	n.flag, _ = page.GetFlag()
	n.numRecs, _ = page.GetNumRecs()
}

func (m *model) updateFlattened() {
	m.items = nil
	m.flatten(m.root)
}

func (m *model) flatten(n *node) {
	m.items = append(m.items, navItem{node: n})
	if n.expanded {
		if n.isLeaf {
			for i := range n.entries {
				m.items = append(m.items, navItem{node: n, entry: &n.entries[i], entryIndex: i})
			}
		} else {
			for _, child := range n.children {
				m.flatten(child)
			}
		}
	}
}

func (m model) Init() tea.Cmd {
	return tea.WindowSize()
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m, tea.Quit
		case "up":
			if m.cursorItem > 0 {
				m.cursorItem--
				m.verificationResult = ""
				m.updateViewport()
			}
		case "down":
			if m.cursorItem < len(m.items)-1 {
				m.cursorItem++
				m.verificationResult = ""
				m.updateViewport()
			}
		case "right":
			item := m.items[m.cursorItem]
			if item.entry == nil && !item.node.expanded {
				item.node.expanded = true
				m.loadNode(item.node)
				m.updateFlattened()
				// Move to first child/entry automatically
				if m.cursorItem < len(m.items)-1 {
					m.cursorItem++
				}
				m.updateViewport()
			}
		case "left":
			item := m.items[m.cursorItem]
			if item.entry != nil {
				// Move back to the leaf node
				for i := m.cursorItem; i >= 0; i-- {
					if m.items[i].node == item.node && m.items[i].entry == nil {
						m.cursorItem = i
						break
					}
				}
				m.updateViewport()
			} else if item.node.expanded {
				item.node.expanded = false
				m.updateFlattened()
				m.updateViewport()
			} else if item.node.parent != nil {
				// Focus parent
				for i, it := range m.items {
					if it.node == item.node.parent && it.entry == nil {
						m.cursorItem = i
						break
					}
				}
				m.updateViewport()
			}
		case "enter":
			item := m.items[m.cursorItem]
			if item.entry != nil && item.node.isLeaf {
				// Show actual row data for leaf entries
				m.showRowData(item.entry)
			} else if item.entry == nil {
				// Expand/collapse nodes
				item.node.expanded = !item.node.expanded
				if item.node.expanded {
					m.loadNode(item.node)
					m.updateFlattened()
					if m.cursorItem < len(m.items)-1 {
						m.cursorItem++
					}
				} else {
					m.updateFlattened()
				}
				m.updateViewport()
			}
		case "v":
			item := m.items[m.cursorItem]
			if item.entry != nil && item.node.isLeaf {
				// Show actual row data for leaf entries
				m.showRowData(item.entry)
			}
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.updateViewport()
		return m, nil
	}

	// Handle viewport messages (mouse wheel, etc.)
	var cmd tea.Cmd
	m.vp, cmd = m.vp.Update(msg)
	return m, cmd
}

// updateViewport updates the viewport content and ensures cursor is visible
func (m *model) updateViewport() {
	if m.height == 0 {
		return
	}

	// Calculate available height for items (header + footer)
	// Header: title (1) + info line (1) = 2 lines
	// Footer: 1 line
	headerHeight := 2
	footerHeight := 1
	availableHeight := m.height - headerHeight - footerHeight

	if availableHeight <= 0 {
		return
	}

	// Update viewport dimensions
	m.vp.Width = m.width
	m.vp.Height = availableHeight

	// Update content
	m.vp.SetContent(m.renderItems())

	// Ensure cursor is visible by adjusting viewport offset
	// Each item is one line, so we need to scroll to show the cursor line
	cursorLine := m.cursorItem
	viewportTop := m.vp.YOffset
	viewportBottom := viewportTop + availableHeight - 1

	if cursorLine < viewportTop {
		// Cursor is above viewport, scroll up to show cursor at top
		m.vp.SetYOffset(cursorLine)
	} else if cursorLine >= viewportBottom {
		// Cursor is below viewport, scroll down to show cursor at bottom
		m.vp.SetYOffset(cursorLine - availableHeight + 1)
	}
}

// renderItems renders all items as a string
func (m *model) renderItems() string {
	var s strings.Builder
	for i, it := range m.items {
		n := it.node
		indent := strings.Repeat("  ", n.level)

		// Use fixed-width prefix for alignment
		prefixStr := "  "
		infoStyle := subtleStyle
		if i == m.cursorItem {
			prefixStr = "> "
			infoStyle = focusedStyle
		}

		if it.entry != nil {
			// This is a record line
			e := it.entry
			connector := "├"
			// Check if it's the last entry for this leaf
			if it.entryIndex == len(n.entries)-1 {
				connector = "└"
			}
			s.WriteString(fmt.Sprintf("%s  %s%s %v -> RID: {%d, %d}\n", prefixStr, indent, subtleStyle.Render(connector), e.val, e.rid.Block(), e.rid.Slot()))
			continue
		}

		// This is a node line
		icon := "▸"
		if n.isLeaf {
			icon = "•"
		} else if n.expanded {
			icon = "▾"
		}

		nodeStyle := internalStyle
		if n.isLeaf {
			nodeStyle = leafStyle
		}

		nodeLine := fmt.Sprintf("%s%s %s %s Block %d", prefixStr, indent, icon, nodeStyle.Render(fmt.Sprintf("%-8s", func() string {
			if n.isLeaf {
				return "Leaf"
			}
			return "Internal"
		}())), n.blkNum)

		recsInfo := fmt.Sprintf(" [%d recs]", n.numRecs)

		rangeInfo := ""
		if n.parent != nil && !n.parent.isLeaf {
			// Find which child index this is to determine key range
			childIdx := -1
			for j, c := range n.parent.children {
				if c == n {
					childIdx = j
					break
				}
			}
			if childIdx != -1 {
				if childIdx == 0 {
					if len(n.parent.entries) > 1 {
						rangeInfo = fmt.Sprintf(" [key < %v]", n.parent.entries[1].val)
					}
				} else if childIdx == len(n.parent.children)-1 {
					rangeInfo = fmt.Sprintf(" [key >= %v]", n.parent.entries[childIdx].val)
				} else {
					rangeInfo = fmt.Sprintf(" [%v <= key < %v]", n.parent.entries[childIdx].val, n.parent.entries[childIdx+1].val)
				}
			}
		}

		s.WriteString(nodeLine + infoStyle.Render(recsInfo) + " " + rangeStyle.Render(rangeInfo) + "\n")
	}
	// Remove trailing newline to avoid extra spacing
	content := s.String()
	if len(content) > 0 && content[len(content)-1] == '\n' {
		content = content[:len(content)-1]
	}
	return content
}

func (m *model) showRowData(e *entry) {
	if m.tableLayout == nil {
		m.verificationResult = "Error: Table layout not found"
		return
	}

	ts, err := table.NewTableScan(m.v.tx, m.tableLayout, m.tableName)
	if err != nil {
		m.verificationResult = fmt.Sprintf("Error creating scan: %v", err)
		return
	}
	defer ts.Close()

	err = ts.MoveToRID(&e.rid)
	if err != nil {
		m.verificationResult = fmt.Sprintf("Error moving to RID: %v", err)
		return
	}

	// Format row data nicely
	var fields []string
	for _, fld := range m.tableLayout.GetSchema().Fields() {
		val, _ := ts.GetValue(fld)
		fields = append(fields, fmt.Sprintf("%s=%v", fld, val))
	}
	m.verificationResult = fmt.Sprintf("RID {%d, %d}: %s", e.rid.Block(), e.rid.Slot(), strings.Join(fields, ", "))
}

func (m model) View() string {
	var s strings.Builder

	s.WriteString(titleStyle.Render(fmt.Sprintf(" CraneDB B+Tree Explorer: %s ", m.indexName)) + "\n")

	// Show current item info safely
	infoText := ""
	if len(m.items) > 0 && m.cursorItem < len(m.items) {
		infoText = fmt.Sprintf(" Table: %s | Block: %d | Level: %d", m.tableName, m.items[m.cursorItem].node.blkNum, m.items[m.cursorItem].node.level)
	} else {
		infoText = fmt.Sprintf(" Table: %s", m.tableName)
	}
	s.WriteString(subtleStyle.Render(infoText) + "\n")

	// Render viewport content - it will fill the available height
	viewportContent := m.vp.View()
	s.WriteString(viewportContent)

	// Build footer with three sections: actions (left), verification (middle), scroll (right)
	footer := m.renderFooter()
	s.WriteString("\n" + footer)

	return s.String()
}

// renderFooter creates a three-column footer: actions (left), verification (middle), scroll (right)
func (m *model) renderFooter() string {
	if m.width == 0 {
		return ""
	}

	// Left: Actions/Help text
	helpText := " ↑/↓: navigate • ←: collapse/up • →/Enter: expand"
	if len(m.items) > 0 && m.cursorItem < len(m.items) {
		item := m.items[m.cursorItem]
		if item.entry != nil && item.node.isLeaf {
			helpText += "/show row data"
		} else {
			helpText += "/drill down"
		}
	}
	helpText += " • q: quit"
	leftSection := titleStyle.Background(lipgloss.Color("0")).Foreground(lipgloss.Color("245")).Render(helpText)

	// Middle: Data verification
	middleSection := ""
	if m.verificationResult != "" {
		middleSection = m.verificationResult
	}

	// Right: Scroll indicator
	rightSection := ""
	if m.height > 0 {
		headerHeight := 2
		footerHeight := 1
		availableHeight := m.height - headerHeight - footerHeight
		if len(m.items) > availableHeight && availableHeight > 0 {
			startIdx := m.vp.YOffset + 1
			endIdx := startIdx + availableHeight - 1
			if endIdx > len(m.items) {
				endIdx = len(m.items)
			}
			rightSection = subtleStyle.Render(fmt.Sprintf("[%d-%d/%d]", startIdx, endIdx, len(m.items)))
		}
	}

	// Calculate widths for proper alignment
	leftWidth := lipgloss.Width(leftSection)
	middleWidth := lipgloss.Width(middleSection)
	rightWidth := lipgloss.Width(rightSection)

	// Calculate available space
	availableWidth := m.width
	if availableWidth <= 0 {
		availableWidth = 80 // fallback
	}

	// Distribute space: left gets what it needs, middle gets remaining, right gets what it needs
	// If not enough space, prioritize left and right
	remainingWidth := availableWidth - leftWidth - rightWidth
	if remainingWidth < 0 {
		remainingWidth = 0
	}

	// Truncate middle section if needed (using lipgloss width-aware truncation)
	if remainingWidth > 0 && middleWidth > remainingWidth {
		truncateStyle := lipgloss.NewStyle().
			Width(remainingWidth).
			MaxWidth(remainingWidth)
		middleSection = truncateStyle.Render(middleSection)
	} else if remainingWidth <= 0 {
		middleSection = ""
	}

	// Build footer with proper alignment
	leftStyle := lipgloss.NewStyle().Width(leftWidth)
	middleStyle := lipgloss.NewStyle().
		Width(remainingWidth).
		Align(lipgloss.Center)
	rightStyle := lipgloss.NewStyle().
		Width(rightWidth).
		Align(lipgloss.Right)

	footer := lipgloss.JoinHorizontal(
		lipgloss.Left,
		leftStyle.Render(leftSection),
		middleStyle.Render(middleSection),
		rightStyle.Render(rightSection),
	)

	return footer
}
