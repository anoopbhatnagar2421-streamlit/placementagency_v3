import streamlit as st
import pandas as pd
from sheets_connector import fetch_candidates_data, get_column_headers, get_unique_values, apply_filter


def render_filter_section():
    """Main filter UI rendering for candidates"""
    
    # Initialize session state for filters
    if 'filters' not in st.session_state:
        st.session_state.filters = []
    if 'filtered_df' not in st.session_state:
        st.session_state.filtered_df = None
    if 'show_new_filter' not in st.session_state:
        st.session_state.show_new_filter = True
    
    # Header
    st.markdown("### Filter Candidates")
    # Clear All Filters button (NEW)
    if st.session_state.filters:
        col1, col2, col3 = st.columns([2, 2, 1])
        with col3:
            if st.button("🗑️ Clear All Filters", key="clear_all_candidate_filters", type="secondary"):
                st.session_state.filters = []
                st.session_state.filtered_df = None
                st.session_state.show_new_filter = True
                st.success("✅ All filters cleared!")
                st.rerun()
   
    
    # Get original data
    sheet_url = "https://docs.google.com/spreadsheets/d/1rpuXdpfwjy0BQcaZcn0Acbh-Se6L3PvyNGiNu4NLcPA/edit?gid=1282014632#gid=1282014632"
    original_df = fetch_candidates_data(sheet_url, "Candidates")
    
    if original_df is None:
        st.error("Cannot fetch data. Check credentials and sheet URL.")
        return
    
    # 🆕 EXCLUDE SELECTED CANDIDATES - Case-insensitive
    if 'Status' in original_df.columns:
        # Case-insensitive check for 'selected' status
        selected_mask = original_df['Status'].str.lower() == 'selected'
        original_df = original_df[~selected_mask]
    
    if len(original_df) == 0:
        st.warning("⚠️ All candidates are already selected. No candidates available for filtering.")
        return
    
    # Get all headers
    headers = get_column_headers(original_df)
    
    # Display applied filters (read-only display)
    if st.session_state.filters:
        st.markdown("**Applied Filters:**")
        
        for i, filter_item in enumerate(st.session_state.filters):
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                st.write(f"Filter {i+1}: {filter_item['column']}")
            
            with col2:
                st.write(f"= {filter_item['value']}")
            
            with col3:
                if st.button("Remove", key=f"remove_{i}"):
                    st.session_state.filters.pop(i)
                    st.rerun()
        
        st.markdown("---")
    
    # Calculate filtered data based on applied filters
    working_df = original_df.copy()
    for filter_item in st.session_state.filters:
        working_df = apply_filter(working_df, filter_item['column'], filter_item['value'])
    
    st.session_state.filtered_df = working_df
    
    # Show new filter input ONLY if show_new_filter is True
    if st.session_state.show_new_filter:
        st.markdown("**Add Filter:**")
        
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            selected_column = st.selectbox(
                "Select Column",
                headers,
                key="new_filter_column"
            )
        
        with col2:
            if selected_column:
                unique_vals = get_unique_values(working_df, selected_column)
                selected_value = st.selectbox(
                    "Select Value",
                    unique_vals,
                    key="new_filter_value"
                )
            else:
                selected_value = None
                st.selectbox("Select Value", [], disabled=True, key="new_filter_value_disabled")
        
        with col3:
            if st.button("Apply", key="apply_filter"):
                if selected_column and selected_value is not None:
                    # Add to filters list
                    st.session_state.filters.append({
                        'column': selected_column,
                        'value': selected_value
                    })
                    # Hide the input form after applying
                    st.session_state.show_new_filter = False
                    st.rerun()
        
        st.markdown("---")
    
    # Add More Filter button - only show when no active filter input
    if not st.session_state.show_new_filter:
        if st.button("➕ Add More Filter", key="add_more_btn", use_container_width=False):
            st.session_state.show_new_filter = True
            st.rerun()
    
    # Display filtered results
    st.markdown("---")
    st.markdown(f"### Filtered Candidates ({len(st.session_state.filtered_df)} records)")
    st.info("ℹ️ Showing only candidates with Status: Pending, Demo, Hold, Rejected (Selected candidates are excluded)")
    
    if len(st.session_state.filtered_df) > 0:
        # Display filtered data in table format
        st.dataframe(
            st.session_state.filtered_df,
            use_container_width=True,
            height=300
        )
        
        # Download button
        csv = st.session_state.filtered_df.to_csv(index=False)
        st.download_button(
            label="Download Filtered List (CSV)",
            data=csv,
            file_name="filtered_candidates.csv",
            mime="text/csv"
        )
    else:
        st.info("No candidates match the selected filters.")


def render():
    """Main entry point for filter module"""
    render_filter_section()