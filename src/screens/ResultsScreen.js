import React, { useCallback, useEffect, useState } from 'react';
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

const ResultsScreen = ({ route }) => {
  const { searchTerm, userId = 1 } = route.params || {};
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [lastUpdated, setLastUpdated] = useState(null);
  const [polling, setPolling] = useState(false);

  const fetchResults = useCallback(async (isPolling = false) => {
    if (!searchTerm) {
      setError('Missing search term. Go back and try again.');
      setLoading(false);
      return;
    }

    // Don't show loading spinner during polling (less disruptive)
    if (!isPolling) {
      setLoading(true);
    }
    setError('');

    try {
      const response = await fetch(
        `${API_BASE_URL}/api/results?search_term=${encodeURIComponent(searchTerm)}&user_id=${userId}`
      );
      const data = await response.json();

      if (!response.ok) {
        throw new Error(data?.detail || 'Failed to load results');
      }

      const newItems = data?.items || [];
      
      // Always update items when we get new results
      // During polling, always update to catch pipeline completion even if count stays same
      if (newItems.length > 0) {
        setItems((prevItems) => {
          const countChanged = newItems.length !== prevItems.length;
          
          // Update if count changed, or during polling (to catch any updates from pipeline)
          if (countChanged || isPolling) {
            setLastUpdated(new Date());
            // Stop polling once we have substantial results (pipeline likely complete)
            if (isPolling && newItems.length >= 10) {
              setPolling(false);
            }
            return newItems;
          }
          return prevItems;
        });
      }
    } catch (err) {
      if (!isPolling) {
        setError(err.message);
      }
    } finally {
      if (!isPolling) {
        setLoading(false);
      }
    }
  }, [searchTerm, userId]);

  // Initial fetch
  useEffect(() => {
    fetchResults(false);
  }, [searchTerm, userId]);

  // Auto-refresh polling after initial load (pipeline takes ~2-3 minutes)
  // Poll for updates to catch when pipeline completes
  useEffect(() => {
    if (!searchTerm || loading) return;

    // Always poll after initial load to catch pipeline completion
    setPolling(true);
    const pollInterval = setInterval(() => {
      fetchResults(true);
    }, 10000); // Poll every 10 seconds

    // Stop polling after 5 minutes (pipeline should be done by then)
    const timeout = setTimeout(() => {
      clearInterval(pollInterval);
      setPolling(false);
    }, 300000); // 5 minutes

    return () => {
      clearInterval(pollInterval);
      clearTimeout(timeout);
      setPolling(false);
    };
  }, [searchTerm, loading, fetchResults]);

  const handleOpenProduct = (url) => {
    if (!url) {
      return;
    }
    Linking.openURL(url).catch(() => {});
  };

  const renderItem = ({ item }) => (
    <TouchableOpacity style={styles.card} onPress={() => handleOpenProduct(item.link || item.product_url)}>
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
          <Text style={styles.price}>${item.price?.toFixed?.(2) ?? '—'}</Text>
          <View style={styles.metaRow}>
            <Text style={styles.metaText}>Rating: {item.rating?.toFixed?.(1) ?? '—'}</Text>
            <Text style={styles.metaText}>Reviews: {item.ratings_total ?? '—'}</Text>
          </View>
          <View style={styles.metaRow}>
            <Text style={styles.metaText}>Score: {item.reccd_score?.toFixed?.(2) ?? '—'}</Text>
            <Text style={styles.metaText}>Rank: {item.search_rank ?? '—'}</Text>
          </View>
        </View>
      </View>
    </TouchableOpacity>
  );

  if (loading) {
    return (
      <SafeAreaView style={styles.centered}>
        <ActivityIndicator size="large" color="#FF9900" />
        <Text style={styles.statusText}>Loading latest rankings…</Text>
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

  const ListHeader = () => (
    <View style={styles.headerContainer}>
      <Text style={styles.header}>Results for "{searchTerm}"</Text>
      {lastUpdated && (
        <Text style={styles.subheader}>Updated {lastUpdated.toLocaleTimeString()}</Text>
      )}
    </View>
  );

  // Use ScrollView for web (better scrolling), FlatList for native (better performance)
  if (Platform.OS === 'web') {
    return (
      <View style={styles.webContainer}>
        <ScrollView
          style={styles.webScrollView}
          contentContainerStyle={styles.listContent}
          refreshControl={
            <RefreshControl refreshing={loading} onRefresh={fetchResults} />
          }
          showsVerticalScrollIndicator={true}
          nestedScrollEnabled={true}
        >
          <ListHeader />
          {items.length === 0 ? (
            <Text style={styles.empty}>No results yet. Pull to refresh.</Text>
          ) : (
            items.map((item, index) => (
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
        data={items}
        keyExtractor={(item, index) => `${item.asin || index}`}
        renderItem={renderItem}
        ListHeaderComponent={ListHeader}
        contentContainerStyle={styles.listContent}
        style={styles.list}
        refreshControl={
          <RefreshControl refreshing={loading} onRefresh={fetchResults} />
        }
        ListEmptyComponent={<Text style={styles.empty}>No results yet. Pull to refresh.</Text>}
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
  empty: {
    textAlign: 'center',
    color: '#4A5568',
    marginTop: 40,
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
