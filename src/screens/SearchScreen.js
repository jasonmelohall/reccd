import React, { useState } from 'react';
import {
  ActivityIndicator,
  KeyboardAvoidingView,
  Platform,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from 'react-native';
import Constants from 'expo-constants';

const API_BASE_URL =
  Constants?.expoConfig?.extra?.apiBaseUrl ??
  Constants?.manifest?.extra?.apiBaseUrl ??
  'https://reccd-web-service.onrender.com';

const DEFAULT_USER_ID = 1;

const SearchScreen = ({ navigation }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [statusMessage, setStatusMessage] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSearch = async () => {
    const trimmed = searchTerm.trim();
    if (!trimmed) {
      setError('Enter a search term to continue.');
      return;
    }

    setError('');
    setStatusMessage('');
    setLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/api/search`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          search_term: trimmed,
          user_id: DEFAULT_USER_ID,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data?.detail || 'Request failed');
      }

      setStatusMessage(data?.message || 'Analyzing products...');

      navigation.navigate('Results', {
        searchTerm: trimmed,
        userId: DEFAULT_USER_ID,
        status: data?.status,
        itemsFound: data?.items_found,
      });
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <KeyboardAvoidingView
      style={styles.container}
      behavior={Platform.OS === 'ios' ? 'padding' : undefined}
    >
      <View style={styles.card}>
        <Text style={styles.title}>Find Amazon best-sellers</Text>
        <Text style={styles.subtitle}>
          Reccd runs the full ML pipeline (Rainforest + Keepa + regression) every time you search.
        </Text>

        <TextInput
          style={styles.input}
          placeholder="e.g. champagne gold bathroom trash can"
          value={searchTerm}
          onChangeText={setSearchTerm}
          autoCorrect={false}
          autoCapitalize="none"
          returnKeyType="search"
          onSubmitEditing={handleSearch}
        />

        <TouchableOpacity style={styles.button} onPress={handleSearch} disabled={loading}>
          {loading ? (
            <ActivityIndicator color="#fff" />
          ) : (
            <Text style={styles.buttonText}>Run Search</Text>
          )}
        </TouchableOpacity>

        {!!statusMessage && <Text style={styles.status}>{statusMessage}</Text>}
        {!!error && <Text style={styles.error}>{error}</Text>}
      </View>
    </KeyboardAvoidingView>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#232F3E',
    justifyContent: 'center',
    paddingHorizontal: 20,
  },
  card: {
    backgroundColor: '#fff',
    borderRadius: 16,
    padding: 24,
    shadowColor: '#000',
    shadowOpacity: 0.1,
    shadowRadius: 20,
    shadowOffset: { width: 0, height: 10 },
    elevation: 5,
  },
  title: {
    fontSize: 24,
    fontWeight: 'bold',
    marginBottom: 8,
    color: '#111',
  },
  subtitle: {
    fontSize: 15,
    color: '#4A5568',
    marginBottom: 20,
  },
  input: {
    borderWidth: 1,
    borderColor: '#E2E8F0',
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 12,
    fontSize: 16,
    marginBottom: 16,
  },
  button: {
    backgroundColor: '#FF9900',
    borderRadius: 12,
    paddingVertical: 14,
    alignItems: 'center',
  },
  buttonText: {
    color: '#111',
    fontWeight: '600',
    fontSize: 16,
  },
  status: {
    marginTop: 16,
    color: '#2F855A',
    fontWeight: '500',
  },
  error: {
    marginTop: 16,
    color: '#E53E3E',
    fontWeight: '500',
  },
});

export default SearchScreen;
