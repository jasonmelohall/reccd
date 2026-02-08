#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OpenAI service for GenAI search: generate Amazon search terms from user need.
Uses same model/pattern as diversity scripts (audio/video).
"""

import ast
import logging
import os
import re
from typing import List

from openai import OpenAI

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


def generate_search_terms(user_input: str, num_terms: int = 3) -> List[str]:
    """
    Call OpenAI to generate targeted Amazon search queries from a paragraph.
    On failure or invalid response, returns [user_input] so search can proceed.

    Args:
        user_input: Paragraph-style product description.
        num_terms: Number of search terms to generate (1-10; caller should clamp).

    Returns:
        List of search term strings.
    """
    if not client:
        logger.warning("OPENAI_API_KEY not set; using user_input as single term")
        return [user_input.strip()] if user_input.strip() else []

    num_terms = max(1, min(10, num_terms))
    prompt = f'''Given this user need: "{user_input.strip()}"

Generate exactly {num_terms} targeted Amazon search queries that will find relevant products.

Rules:
- Each term: 2-7 words
- Include qualifiers only if user specified (eco-friendly, waterproof, etc.)
- No brand names unless explicitly requested
- Focus on different aspects/variations of the need

Output ONLY a Python list:
["term 1", "term 2", "term 3"]'''

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You output only a Python list of strings. No other text."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=500,
        )
        raw = (response.choices[0].message.content or "").strip()
        if not raw:
            return [user_input.strip()] if user_input.strip() else []

        # Strip code fences if present
        if raw.startswith("```"):
            raw = re.sub(r"^```\w*\n?", "", raw)
        if raw.endswith("```"):
            raw = raw[:-3].strip()
        raw = raw.strip()

        # Extract list: find [ ... ]
        list_match = re.search(r"\[[\s\S]*\]", raw)
        if list_match:
            raw = list_match.group(0)
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, list) and all(isinstance(x, str) for x in parsed):
            terms = [t.strip() for t in parsed if t and isinstance(t, str)]
            if terms:
                return terms[:num_terms]
    except Exception as e:
        logger.warning("OpenAI generate_search_terms failed: %s", e)

    return [user_input.strip()] if user_input.strip() else []
