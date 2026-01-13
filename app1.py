# Full updated HL7 Editor App with precision filtering and editing
# Streamlit UI for loading HL7 files, exploring segment/field values, defining edit groups, and applying scoped edits.

import streamlit as st
import re
from collections import defaultdict, Counter
from zipfile import ZipFile
from io import BytesIO


# HL7 Parsing and Processing

def split_hl7_messages(raw_text):
    """
    Split a raw HL7 text blob into individual messages.
    Assumes each message begins with MSH| and may be CR or LF separated.
    """
    chunks = re.split(r'(?=MSH\|)', raw_text.replace('\r', '\n'))
    return [chunk.strip() for chunk in chunks if chunk.strip()]


def handle_multiple_uploads(uploaded_files):
    """
    Read all uploaded files, decode them, split into HL7 messages,
    and return one combined list of messages.
    """
    all_messages = []
    for file in uploaded_files:
        raw = file.read().decode("utf-8", errors="ignore")
        msgs = split_hl7_messages(raw)
        all_messages.extend(msgs)
    return all_messages


def get_segment_field_map(messages):
    """
    Build a dictionary of:
      segment_name -> list of available field indices (including component indices when caret-delimited)

    This is used to drive the Field dropdown per segment in the UI.
    """
    segments = defaultdict(set)

    for msg in messages:
        for line in msg.split('\n'):
            if not line.strip():
                continue

            parts = line.split('|')
            seg_name = parts[0]

            for i in range(1, len(parts)):
                field = parts[i]

                # If the field contains components (^), expose sub-field selectors like 5.1, 5.2, etc.
                if '^' in field:
                    comps = field.split('^')
                    for j in range(1, len(comps) + 1):
                        segments[seg_name].add(f"{i}.{j}")
                else:
                    segments[seg_name].add(f"{i}")

    # Sort numerically (field number first, then component number)
    return {
        seg: sorted(
            fields,
            key=lambda f: (int(f.split('.')[0]), int(f.split('.')[1]) if '.' in f else 0)
        )
        for seg, fields in segments.items()
    }


def get_value_counts(messages, segment, field):
    """
    Count distinct values for a given segment+field across all messages.
    Used for the Value dropdown so the user can pick exact-match filter values.
    """
    counts = Counter()

    for msg in messages:
        for line in msg.split('\n'):
            if line.startswith(segment + '|'):
                parts = line.split('|')

                idx = field.split('.')
                field_index = int(idx[0])
                comp_index = int(idx[1]) if len(idx) > 1 else None

                if field_index < len(parts):
                    val = parts[field_index]

                    # Component extraction when field selector includes .x
                    if comp_index:
                        comps = val.split('^')
                        if comp_index <= len(comps):
                            val = comps[comp_index - 1]
                        else:
                            val = ""

                    counts[val] += 1

    return dict(counts)



# Filtering Logic

def segment_line_matches(parts, field_filters):
    """
    Evaluate one segment line (already split by | into 'parts') against a list of field filters.
    Filters are exact match only, and can target a component (e.g., 5.2).
    """
    for field, expected in field_filters:
        idx = field.split('.')
        f_idx = int(idx[0])
        c_idx = int(idx[1]) if len(idx) > 1 else None

        if f_idx >= len(parts):
            return False

        val = parts[f_idx]

        if c_idx:
            comps = val.split('^')
            if c_idx > len(comps):
                return False
            val = comps[c_idx - 1]

        if val != expected:
            return False

    return True


def message_satisfies_filters_exact_lines(message, filters):
    """
    Determine whether a message satisfies the filters.
    Filters apply at the segment-line level.
    A message is considered a match if for each segment involved, at least one line matches all filters for that segment.
    """
    grouped = defaultdict(list)

    # Group lines by segment type so we can evaluate per-segment conditions
    for line in message.split('\n'):
        if not line.strip():
            continue
        parts = line.split('|')
        seg_type = parts[0]
        grouped[seg_type].append(parts)

    matched_keys = set()

    # Organize filters by segment for cleaner per-line matching
    filters_by_segment = defaultdict(list)
    for seg, field, val in filters:
        filters_by_segment[seg].append((field, val))

    # For each segment in filters, require at least one matching line
    for seg, seg_filters in filters_by_segment.items():
        seg_lines = grouped.get(seg, [])

        for i, parts in enumerate(seg_lines):
            if segment_line_matches(parts, seg_filters):
                matched_keys.add((seg, str(i)))
                break

        # If no lines matched for this segment, message fails
        if not matched_keys:
            return False, set()

    return True, matched_keys



# Editing Logic


def apply_bulk_edits_exact_lines(messages, edits_by_filter_group):
    """
    Apply grouped edits to messages.

    Each edit group has:
      filters: list of (seg, field, expected_value)
      edits:   list of (seg, field, new_value)

    For each line:
      If the line's segment matches and the line satisfies all filters for that group,
      then apply that group's edits to that line.
    """
    edited_messages = []

    for message in messages:
        lines = message.split('\n')
        output_lines = []

        for line in lines:
            if not line.strip():
                output_lines.append(line)
                continue

            parts = line.split('|')
            seg_type = parts[0]
            original_parts = parts[:]
            was_edited = False

            # Try every edit group for this line
            for edit_group in edits_by_filter_group:
                group_filters = [f for f in edit_group["filters"] if f[0] == seg_type]
                group_edits = [e for e in edit_group["edits"] if e[0] == seg_type]

                # If this group doesn't apply to this segment type, skip
                if not group_filters or not group_edits:
                    continue

                # Check if current line matches all filters for this group
                match = True
                for _, field, expected in group_filters:
                    idx = field.split('.')
                    f_idx = int(idx[0])
                    c_idx = int(idx[1]) if len(idx) > 1 else None

                    if f_idx >= len(parts):
                        match = False
                        break

                    val = parts[f_idx]

                    if c_idx:
                        comps = val.split('^')
                        if c_idx > len(comps):
                            match = False
                            break
                        val = comps[c_idx - 1]

                    if val != expected:
                        match = False
                        break

                # Apply edits if match
                if match:
                    for _, field, new_val in group_edits:
                        idx = field.split('.')
                        f_idx = int(idx[0])
                        c_idx = int(idx[1]) if len(idx) > 1 else None

                        if f_idx >= len(parts):
                            continue

                        # Support component edits by expanding caret list to required length
                        if c_idx:
                            comps = parts[f_idx].split('^')
                            while len(comps) < c_idx:
                                comps.append('')
                            comps[c_idx - 1] = '' if new_val.lower() == 'delete' else new_val
                            parts[f_idx] = '^'.join(comps)
                        else:
                            parts[f_idx] = '' if new_val.lower() == 'delete' else new_val

                    was_edited = True

            output_lines.append('|'.join(parts) if was_edited else '|'.join(original_parts))

        edited_messages.append('\n'.join(output_lines))

    return edited_messages



# Diff Highlighting

def highlight_diff(before, after, edits):
    """
    Highlight changed fields between two messages using basic HTML markup.
    Only attempts highlighting for segments/fields included in the edits list.
    """
    before_lines = before.split('\n')
    after_lines = after.split('\n')

    for i, (b_line, a_line) in enumerate(zip(before_lines, after_lines)):
        if b_line == a_line:
            continue

        # Only highlight lines for segments that appear in the edit list
        if not any(b_line.startswith(seg + '|') for seg, _, _ in edits):
            continue

        parts_b = b_line.split('|')
        parts_a = a_line.split('|')

        for seg, field, _ in edits:
            if not b_line.startswith(seg + '|'):
                continue

            idx = field.split('.')
            f_idx = int(idx[0])
            c_idx = int(idx[1]) if len(idx) > 1 else None

            if f_idx < len(parts_b) and f_idx < len(parts_a):
                vb = parts_b[f_idx]
                va = parts_a[f_idx]

                if c_idx:
                    vb_parts = vb.split('^')
                    va_parts = va.split('^')

                    if c_idx <= len(vb_parts) and c_idx <= len(va_parts):
                        vb_parts[c_idx - 1] = f"<span style='background-color:#ffeeba;'>{vb_parts[c_idx - 1]}</span>"
                        va_parts[c_idx - 1] = f"<span style='background-color:#c3e6cb;'>{va_parts[c_idx - 1]}</span>"
                        parts_b[f_idx] = '^'.join(vb_parts)
                        parts_a[f_idx] = '^'.join(va_parts)
                else:
                    parts_b[f_idx] = f"<span style='background-color:#ffeeba;'>{vb}</span>"
                    parts_a[f_idx] = f"<span style='background-color:#c3e6cb;'>{va}</span>"

        before_lines[i] = '|'.join(parts_b)
        after_lines[i] = '|'.join(parts_a)

    return '\n'.join(before_lines), '\n'.join(after_lines)



# Streamlit App UI


st.title("🧬 HL7 Message Editor")

# File upload entry point
uploaded_files = st.file_uploader(
    "Upload HL7/TXT files",
    type=["hl7", "txt"],
    accept_multiple_files=True
)

if uploaded_files:
    # Load and split all HL7 messages from all uploads
    messages = handle_multiple_uploads(uploaded_files)
    st.success(f"✅ Loaded {len(messages)} messages across {len(uploaded_files)} files")

    # Build UI field map for segment/field dropdowns
    seg_map = get_segment_field_map(messages)

    # Simple merged export of all parsed messages
    if st.download_button(
        "⬇️ Download Combined HL7 File",
        data='\n'.join(messages),
        file_name="merged.hl7"
    ):
        st.info("✔️ Merged file downloaded")

    # Partition export for large datasets
    st.subheader("📦 Partition Export")
    num_per_file = st.number_input(
        "Messages per file",
        min_value=1,
        max_value=10000,
        value=1000
    )

    if st.button("Download Partitioned ZIP"):
        zip_buffer = BytesIO()
        with ZipFile(zip_buffer, 'w') as zipf:
            for i in range(0, len(messages), num_per_file):
                chunk = '\n'.join(messages[i:i + num_per_file])
                zipf.writestr(f"hl7_part_{i // num_per_file + 1}.hl7", chunk)

        st.download_button(
            "📁 Download ZIP",
            data=zip_buffer.getvalue(),
            file_name="partitioned_hl7.zip"
        )

    # Edit group configuration
    st.subheader("🔍 Define Edit Groups")
    edit_group_count = st.number_input("How many edit groups?", 1, 10, 1)

    edits_by_filter_group = []

    for group_index in range(edit_group_count):
        st.markdown(f"---\n### 🧪 Edit Group #{group_index + 1}")

        # Filter configuration per group
        group_filters = []
        filter_count = st.number_input(
            f"How many filters for Group #{group_index + 1}?",
            1, 5, 1,
            key=f"filter_ct_{group_index}"
        )

        for i in range(filter_count):
            c1, c2, c3 = st.columns([1.5, 1.5, 2])

            with c1:
                seg = st.selectbox(
                    f"Segment",
                    list(seg_map.keys()),
                    key=f"g{group_index}_seg_{i}"
                )

            with c2:
                field = st.selectbox(
                    "Field",
                    seg_map[seg],
                    key=f"g{group_index}_field_{i}"
                )

            with c3:
                vals = get_value_counts(messages, seg, field)
                options = [f"{v} ({c})" for v, c in sorted(vals.items(), key=lambda x: (-x[1], x[0]))]
                val = st.selectbox(
                    "Value",
                    options,
                    key=f"g{group_index}_val_{i}"
                ).split(" (", 1)[0]

            group_filters.append((seg, field, val))

        # Edit configuration per group
        st.markdown("**✏️ Edit Fields**")
        edit_fields = []
        edit_count = st.number_input(
            f"How many edits for Group #{group_index + 1}?",
            1, 5, 1,
            key=f"edit_ct_{group_index}"
        )

        for i in range(edit_count):
            c1, c2, c3 = st.columns([1.5, 1.5, 3])

            with c1:
                seg = st.selectbox(
                    "Segment",
                    list(seg_map.keys()),
                    key=f"g{group_index}_edit_seg_{i}"
                )

            with c2:
                field = st.selectbox(
                    "Field",
                    seg_map[seg],
                    key=f"g{group_index}_edit_field_{i}"
                )

            with c3:
                new_val = st.text_input(
                    "New value (or DELETE)",
                    key=f"g{group_index}_edit_val_{i}"
                )

            if new_val != "":
                edit_fields.append((seg, field, new_val))

        # Only store groups that are complete
        if group_filters and edit_fields:
            edits_by_filter_group.append({
                "filters": group_filters,
                "edits": edit_fields
            })

    # Determine which messages match all groups
    matched_messages = []
    for msg in messages:
        all_pass = True
        for group in edits_by_filter_group:
            passed, _ = message_satisfies_filters_exact_lines(msg, group["filters"])
            if not passed:
                all_pass = False
                break
        if all_pass:
            matched_messages.append(msg)

    st.info(f"🔎 {len(matched_messages)} messages match all group filters")

    # Preview first matching message before applying globally
    if matched_messages and edits_by_filter_group:
        st.subheader("🔬 First Match Preview (Highlight All Edits)")

        preview_before = matched_messages[0]
        preview_after = apply_bulk_edits_exact_lines([preview_before], edits_by_filter_group)[0]

        all_edits_flat = [edit for group in edits_by_filter_group for edit in group["edits"]]
        before_highlighted, after_highlighted = highlight_diff(preview_before, preview_after, all_edits_flat)

        cb, ca = st.columns(2)

        with cb:
            st.markdown("**Before**")
            st.markdown(
                f"<div style='overflow-x:auto'><pre>{before_highlighted}</pre></div>",
                unsafe_allow_html=True
            )

        with ca:
            st.markdown("**After**")
            st.markdown(
                f"<div style='overflow-x:auto'><pre>{after_highlighted}</pre></div>",
                unsafe_allow_html=True
            )

    # Apply edits to all loaded messages and export
    if st.button("✅ Apply Edits and Download"):
        edited_messages = apply_bulk_edits_exact_lines(messages, edits_by_filter_group)
        st.download_button(
            "⬇️ Download Edited HL7 File",
            data='\n'.join(edited_messages),
            file_name="edited_output.hl7"
        )
