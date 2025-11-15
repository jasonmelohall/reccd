import React, { useCallback, useEffect, useState } from 'react';
import {
  ActivityIndicator,
  FlatList,
  RefreshControl,
  SafeAreaView,
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

  const fetchResults = useCallback(async () => {
    if (!searchTerm) {
      setError('Missing search term. Go back and try again.');
      setLoading(false);
      return;
    }

    setLoading(true);
    setError('');

    try {
      const response = await fetch(
        `${API_BASE_URL}/api/results?search_term=${encodeURIComponent(searchTerm)}&user_id=${userId}`
      );
      const data = await response.json();

      if (!response.ok) {
        throw new Error(data?.detail || 'Failed to load results');
      }

      setItems(data?.items || []);
      setLastUpdated(new Date());
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [searchTerm, userId]);

  useEffect(() => {
    fetchResults();
  }, [fetchResults]);

  const handleOpenProduct = (url) => {
    if (!url) {
      return;
    }
    Linking.openURL(url).catch(() => {});
  };

  const renderItem = ({ item }) => (
    <TouchableOpacity style={styles.card} onPress={() => handleOpenProduct(item.product_url)}>
      <Text style={styles.cardTitle}>{item.title}</Text>
      <Text style={styles.price}>${item.price?.toFixed?.(2) ?? '—'}</Text>
      <View style={styles.metaRow}>
        <Text style={styles.metaText}>Rating: {item.rating ?? '—'}</Text>
        <Text style={styles.metaText}>Reviews: {item.ratings_total ?? '—'}</Text>
      </View>
      <View style={styles.metaRow}>
        <Text style={styles.metaText}>Score: {item.reccd_score?.toFixed?.(2) ?? '—'}</Text>
        <Text style={styles.metaText}>Rank: {item.search_rank ?? '—'}</Text>
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

  return (
    <SafeAreaView style={styles.container}>
      <Text style={styles.header}>Results for “{searchTerm}”</Text>
      {lastUpdated && (
        <Text style={styles.subheader}>Updated {lastUpdated.toLocaleTimeString()}</Text>
      )}
      <FlatList
        data={items}
        keyExtractor={(item, index) => `${item.asin || index}`}
        renderItem={renderItem}
        contentContainerStyle={styles.listContent}
        refreshControl={
          <RefreshControl refreshing={loading} onRefresh={fetchResults} />
        }
        ListEmptyComponent={<Text style={styles.empty}>No results yet. Pull to refresh.</Text>}
      />
    </SafeAreaView>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#F7FAFC',
  },
  listContent: {
    padding: 16,
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
    paddingHorizontal: 16,
    paddingTop: 16,
    color: '#1A202C',
  },
  subheader: {
    fontSize: 14,
    color: '#4A5568',
    paddingHorizontal: 16,
    paddingBottom: 8,
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
