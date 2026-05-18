import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  ActivityIndicator,
  FlatList,
  Image,
  Platform,
  RefreshControl,
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Text,
  TouchableOpacity,
  View,
} from 'react-native';
import Constants from 'expo-constants';
import * as Linking from 'expo-linking';

const API_BASE_URL =
  Constants?.expoConfig?.extra?.apiBaseUrl ??
  Constants?.manifest?.extra?.apiBaseUrl ??
  'https://reccd-web-service.onrender.com';

const PIPELINE_STATUS_MESSAGES = [
  'Searching Amazon…',
  'Gathering product data…',
  'Fetching prices & ratings…',
  'Running rankings…',
  'Almost there—updating every 10 sec…',
];

const ResultsScreen = ({ route }) => {
  const { searchTerm, searchTerms, userId = 1 } = route.params || {};
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [lastUpdated, setLastUpdated] = useState(null);
  const [polling, setPolling] = useState(false);
  const [pipelineStatusIndex, setPipelineStatusIndex] = useState(0);
  const isGenAI = searchTerms && searchTerms.length > 0;
  const [selectedPills, setSelectedPills] = useState(() =>
    isGenAI ? searchTerms.reduce((acc, t) => ({ ...acc, [t]: true }), {}) : {}
  );

  const [pipelineStartTime] = useState(() => Date.now());
  const pipelineInProgress = items.length === 0 && polling;
  const showEmptyState = items.length === 0 && !loading && !polling;
  const pipelineTakingLong = pipelineInProgress && (Date.now() - pipelineStartTime > 120000); // 2 min

  const fetchResults = useCallback(async (isPolling = false) => {
    const hasTerm = searchTerm || (searchTerms && searchTerms.length > 0);
    if (!hasTerm) {
      setError('Missing search term. Go back and try again.');
      setLoading(false);
      return;
    }

    if (!isPolling) setLoading(true);
    setError('');

    try {
      const url = isGenAI
        ? `${API_BASE_URL}/api/results?${searchTerms.map((t) => `search_terms=${encodeURIComponent(t)}`).join('&')}&user_id=${userId}`
        : `${API_BASE_URL}/api/results?search_term=${encodeURIComponent(searchTerm)}&user_id=${userId}`;
      const response = await fetch(url);
      const data = await response.json();

      if (!response.ok) {
        throw new Error(data?.detail || 'Failed to load results');
      }

      const newItems = data?.items || [];

      if (isPolling) pollFailuresRef.current = 0;
      setItems(newItems);
      if (newItems.length > 0) {
        setLastUpdated(new Date());
        if (isPolling && newItems.length >= 10) {
          setPolling(false);
        }
      }
    } catch (err) {
      if (!isPolling) {
        setError(err.message);
      } else {
        pollFailuresRef.current = (pollFailuresRef.current || 0) + 1;
        if (pollFailuresRef.current >= POLL_FAILURES_BEFORE_ERROR) {
          setError('Couldn\'t load results. Pull to refresh.');
          setPolling(false);
        }
      }
    } finally {
      if (!isPolling) {
        setLoading(false);
      }
    }
  }, [searchTerm, searchTerms, userId, isGenAI]);

  const fetchResultsRef = useRef(fetchResults);
  fetchResultsRef.current = fetchResults;
  const pollFailuresRef = useRef(0);
  const POLL_FAILURES_BEFORE_ERROR = 2;

  useEffect(() => {
    if (isGenAI && searchTerms?.length) {
      setSelectedPills((prev) =>
        searchTerms.reduce((acc, t) => ({ ...acc, [t]: prev[t] !== false }), {})
      );
    }
  }, [isGenAI, searchTerms]);

  // Initial fetch
  useEffect(() => {
    fetchResults(false);
  }, [searchTerm, searchTerms, userId]);

  // Rotate pipeline status message every 12s when waiting for results
  useEffect(() => {
    if (!pipelineInProgress) return;
    const interval = setInterval(() => {
      setPipelineStatusIndex((i) => (i + 1) % PIPELINE_STATUS_MESSAGES.length);
    }, 12000);
    return () => clearInterval(interval);
  }, [pipelineInProgress]);

  // Poll every 10s after initial load; use ref so interval is not recreated every render
  useEffect(() => {
    const hasTerm = searchTerm || (searchTerms && searchTerms.length > 0);
    if (!hasTerm || loading) return;

    setPolling(true);
    const firstPollTimer = setTimeout(() => fetchResultsRef.current(true), 5000);
    const pollInterval = setInterval(() => {
      fetchResultsRef.current(true);
    }, 10000);

    const timeout = setTimeout(() => {
      clearInterval(pollInterval);
      setPolling(false);
    }, 300000);

    return () => {
      clearTimeout(firstPollTimer);
      clearInterval(pollInterval);
      clearTimeout(timeout);
      setPolling(false);
    };
  }, [searchTerm, searchTerms, loading]);

  const logClick = async (item) => {
    try {
      await fetch(`${API_BASE_URL}/api/click`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          user_id: userId,
          asin: item.asin,
          parent_asin: item.parent_asin || null,
          title: item.title || null,
          price: item.price || null,
          rating: item.rating || null,
          ratings_total: item.ratings_total || null,
          frequency: item.frequency || null,
          search_rank: item.search_rank || null,
          release_date: item.release_date || null,
          reccd_score: item.reccd_score || null,
          price_percentile: item.price_percentile || null,
          rating_percentile: item.rating_percentile || null,
          release_date_percentile: item.release_date_percentile || null,
          frequency_percentile: item.frequency_percentile || null,
          search_rank_percentile: item.search_rank_percentile || null,
          search_term: item.search_term || searchTerm,
          is_relevant: true,
        }),
      });
    } catch (err) {
      // Silently fail - don't block user from opening product
      console.error('Failed to log click:', err);
    }
  };

  const handleOpenProduct = (item) => {
    if (!item) return;
    const url = item.link || item.product_url;
    if (!url) return;
    if (Platform.OS === 'web') {
      window.open(url, '_blank', 'noopener,noreferrer');
    } else {
      Linking.openURL(url).catch(() => {});
    }
    logClick(item);
  };

  // One search_term per row; API sends search_terms as [that term] for compatibility
  const itemTerms = (item) =>
    (item.search_terms && item.search_terms.length > 0)
      ? item.search_terms
      : (item.search_term ? [item.search_term] : []);
  // GenAI: show item if it has no terms (show all) or its term is selected in pills
  const visibleItems = isGenAI && Object.keys(selectedPills).length > 0
    ? items.filter((item) => {
        const terms = itemTerms(item);
        return terms.length === 0 || terms.some((t) => selectedPills[t]);
      })
    : items;

  const cardContent = (item) => (
    <View style={styles.cardContent}>
      {item.image_url && (
        <Image
          source={{ uri: item.image_url }}
          style={styles.productImage}
          resizeMode="contain"
        />
      )}
      <View style={styles.cardText}>
        <Text style={styles.cardTitle} numberOfLines={2}>{item.title || 'No title'}</Text>
        {isGenAI && itemTerms(item).length > 0 && (
          <View style={styles.badgeRow}>
            {itemTerms(item).map((t) => (
              <View key={t} style={styles.badge}>
                <Text style={styles.badgeText} numberOfLines={1}>{t}</Text>
              </View>
            ))}
          </View>
        )}
        <Text style={styles.price}>${item.price?.toFixed?.(2) ?? '—'}</Text>
        <View style={styles.metaRow}>
          <Text style={styles.metaText}>Rating: {item.rating?.toFixed?.(1) ?? '—'}</Text>
          <Text style={styles.metaText}>Reviews: {item.ratings_total ?? '—'}</Text>
        </View>
        <View style={styles.metaRow}>
          <Text style={styles.metaText}>Score: {item.reccd_score?.toFixed?.(2) ?? '—'}</Text>
          <Text style={styles.metaText}>Rank: {item.search_rank ?? '—'}</Text>
        </View>
        <View style={styles.metaRow}>
          <Text style={styles.metaText}>Frequency: {item.frequency?.toFixed?.(2) ?? '—'}</Text>
          <Text style={styles.metaText}>Release: {item.release_date ?? '—'}</Text>
        </View>
      </View>
    </View>
  );

  const renderItem = ({ item }) => {
    const url = item.link || item.product_url;
    if (Platform.OS === 'web' && url) {
      return (
        <a
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          style={styles.cardLink}
          onClick={() => logClick(item)}
        >
          <View style={styles.card}>{cardContent(item)}</View>
        </a>
      );
    }
    return (
      <TouchableOpacity style={styles.card} onPress={() => handleOpenProduct(item)}>
        {cardContent(item)}
      </TouchableOpacity>
    );
  };

  if (loading && items.length === 0) {
    return (
      <SafeAreaView style={styles.centered}>
        <ActivityIndicator size="large" color="#FF9900" />
        <Text style={styles.statusText}>Loading…</Text>
        <Text style={styles.statusSubtext}>We'll update every 10 seconds.</Text>
      </SafeAreaView>
    );
  }

  if (error) {
    return (
      <SafeAreaView style={styles.centered}>
        <Text style={styles.error}>{error}</Text>
        <TouchableOpacity style={styles.retryButton} onPress={fetchResults}>
          <Text style={styles.retryText}>Try Again</Text>
        </TouchableOpacity>
      </SafeAreaView>
    );
  }

  const pillCount = (term) => items.filter((item) => itemTerms(item).includes(term)).length;

  const ListHeader = () => (
    <View style={styles.headerContainer}>
      <Text style={styles.header}>
        {isGenAI ? 'GenAI results' : `Results for "${searchTerm}"`}
      </Text>
      {lastUpdated && (
        <Text style={styles.subheader}>Updated {lastUpdated.toLocaleTimeString()}</Text>
      )}
      {isGenAI && searchTerms.length > 0 && (
        <View style={styles.pillRow}>
          {searchTerms.map((term) => {
            const selected = selectedPills[term] !== false;
            const count = pillCount(term);
            return (
              <TouchableOpacity
                key={term}
                style={[styles.pill, selected && styles.pillSelected]}
                onPress={() => setSelectedPills((prev) => ({ ...prev, [term]: !prev[term] }))}
              >
                <Text style={[styles.pillText, selected && styles.pillTextSelected]} numberOfLines={1}>
                  {term} ({count})
                </Text>
              </TouchableOpacity>
            );
          })}
        </View>
      )}
    </View>
  );

  if (Platform.OS === 'web') {
    return (
      <View style={styles.webContainer}>
        <ScrollView
          style={styles.webScrollView}
          contentContainerStyle={styles.listContent}
          refreshControl={
            <RefreshControl refreshing={loading} onRefresh={() => fetchResults(false)} />
          }
          showsVerticalScrollIndicator={true}
          nestedScrollEnabled={true}
        >
          <ListHeader />
            {visibleItems.length === 0 ? (
            <View style={styles.emptyContainer}>
              {pipelineInProgress ? (
                <>
                  <ActivityIndicator size="small" color="#FF9900" style={{ marginBottom: 8 }} />
                  <Text style={styles.pipelineStatus}>
                    {PIPELINE_STATUS_MESSAGES[pipelineStatusIndex]}
                  </Text>
                  <Text style={styles.emptySubtext}>Updating every 10 seconds. Pull to refresh.</Text>
                  {pipelineTakingLong && (
                    <Text style={styles.takingLong}>Taking longer than usual. Pull to refresh.</Text>
                  )}
                </>
              ) : showEmptyState ? (
                <Text style={styles.empty}>No results yet. Pull to refresh.</Text>
              ) : null}
            </View>
          ) : (
            visibleItems.map((item, index) => (
              <View key={`${item.asin || index}`}>
                {renderItem({ item })}
              </View>
            ))
          )}
        </ScrollView>
      </View>
    );
  }

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <FlatList
        data={visibleItems}
        keyExtractor={(item, index) => `${item.asin || index}`}
        renderItem={renderItem}
        ListHeaderComponent={ListHeader}
        contentContainerStyle={styles.listContent}
        style={styles.list}
        refreshControl={
          <RefreshControl refreshing={loading} onRefresh={() => fetchResults(false)} />
        }
        ListEmptyComponent={
          <View style={styles.emptyContainer}>
            {pipelineInProgress ? (
              <>
                <ActivityIndicator size="small" color="#FF9900" style={{ marginBottom: 8 }} />
                <Text style={styles.pipelineStatus}>
                  {PIPELINE_STATUS_MESSAGES[pipelineStatusIndex]}
                </Text>
                <Text style={styles.emptySubtext}>Updating every 10 seconds. Pull to refresh.</Text>
                {pipelineTakingLong && (
                  <Text style={styles.takingLong}>Taking longer than usual. Pull to refresh.</Text>
                )}
              </>
            ) : showEmptyState ? (
              <Text style={styles.empty}>No results yet. Pull to refresh.</Text>
            ) : null}
          </View>
        }
        showsVerticalScrollIndicator={true}
        scrollEnabled={true}
        keyboardShouldPersistTaps="handled"
      />
    </SafeAreaView>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#F7FAFC',
  },
  headerContainer: {
    paddingHorizontal: 16,
    paddingTop: 16,
    paddingBottom: 8,
    backgroundColor: '#F7FAFC',
  },
  list: {
    flex: 1,
  },
  listContent: {
    paddingHorizontal: 16,
    paddingBottom: 32,
  },
  webContainer: {
    flex: 1,
    backgroundColor: '#F7FAFC',
    height: '100vh',
    width: '100%',
  },
  webScrollView: {
    flex: 1,
    height: '100%',
  },
  centered: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    padding: 24,
    backgroundColor: '#F7FAFC',
  },
  header: {
    fontSize: 20,
    fontWeight: '700',
    color: '#1A202C',
  },
  subheader: {
    fontSize: 14,
    color: '#4A5568',
    marginTop: 4,
  },
  pillRow: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 8,
    marginTop: 12,
  },
  pill: {
    paddingVertical: 6,
    paddingHorizontal: 12,
    borderRadius: 20,
    backgroundColor: '#E2E8F0',
  },
  pillSelected: {
    backgroundColor: '#FF9900',
  },
  pillText: {
    fontSize: 12,
    color: '#4A5568',
  },
  pillTextSelected: {
    color: '#111',
    fontWeight: '600',
  },
  badgeRow: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 4,
    marginBottom: 6,
  },
  badge: {
    backgroundColor: '#EDF2F7',
    paddingVertical: 2,
    paddingHorizontal: 6,
    borderRadius: 4,
    maxWidth: 120,
  },
  badgeText: {
    fontSize: 10,
    color: '#4A5568',
  },
  cardLink: {
    textDecoration: 'none',
    color: 'inherit',
    display: 'block',
  },
  card: {
    backgroundColor: '#fff',
    borderRadius: 12,
    padding: 16,
    marginBottom: 12,
    shadowColor: '#000',
    shadowOpacity: 0.05,
    shadowOffset: { width: 0, height: 4 },
    shadowRadius: 10,
    elevation: 2,
  },
  cardContent: {
    flexDirection: 'row',
  },
  productImage: {
    width: 100,
    height: 100,
    borderRadius: 8,
    marginRight: 12,
    backgroundColor: '#F7FAFC',
  },
  cardText: {
    flex: 1,
  },
  cardTitle: {
    fontSize: 16,
    fontWeight: '600',
    marginBottom: 6,
    color: '#1A202C',
  },
  price: {
    fontSize: 16,
    fontWeight: '700',
    color: '#DD6B20',
    marginBottom: 8,
  },
  metaRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 4,
  },
  metaText: {
    fontSize: 13,
    color: '#4A5568',
  },
  statusText: {
    marginTop: 12,
    color: '#4A5568',
  },
  statusSubtext: {
    marginTop: 8,
    fontSize: 14,
    color: '#718096',
  },
  emptyContainer: {
    paddingVertical: 40,
    paddingHorizontal: 24,
    alignItems: 'center',
  },
  pipelineStatus: {
    textAlign: 'center',
    fontSize: 16,
    color: '#2D3748',
    fontWeight: '500',
  },
  emptySubtext: {
    marginTop: 8,
    fontSize: 14,
    color: '#718096',
    textAlign: 'center',
  },
  takingLong: {
    marginTop: 12,
    fontSize: 14,
    color: '#C53030',
    fontWeight: '500',
    textAlign: 'center',
  },
  empty: {
    textAlign: 'center',
    color: '#4A5568',
  },
  error: {
    color: '#E53E3E',
    fontSize: 16,
    marginBottom: 16,
    textAlign: 'center',
  },
  retryButton: {
    backgroundColor: '#FF9900',
    paddingHorizontal: 24,
    paddingVertical: 12,
    borderRadius: 10,
  },
  retryText: {
    fontWeight: '600',
    color: '#111',
  },
});

export default ResultsScreen;
