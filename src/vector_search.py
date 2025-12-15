"""
Vector Search Engine Stub for Semantic Similarity.

This module provides stubbed vector search functionality for the exploitation/
similarity feed. It simulates semantic search as described in todo.md for
finding similar videos based on user's watch history.

In production, this would integrate with a vector database like:
- Pinecone, Weaviate, Qdrant, or ChromaDB
- Or use embeddings with PostgreSQL pgvector
"""

import logging
import random
import numpy as np
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class VideoEmbedding:
    """Container for video embeddings."""
    video_id: str
    embedding: np.ndarray
    metadata: Dict


class VectorSearchEngine:
    """
    Vector search engine for semantic similarity.

    As per todo.md: "create exploitation pull: Similarly, pull the videos 10 times
    from vectorDB for the semantic search and finally stop"

    This stub simulates:
    1. Video embeddings generation
    2. Similarity search based on user history
    3. Diversity-aware ranking
    """

    def __init__(self, embedding_dim: int = 384):
        """
        Initialize vector search engine.

        Args:
            embedding_dim: Dimension of video embeddings
        """
        self.embedding_dim = embedding_dim
        self.video_embeddings = {}  # video_id -> embedding
        self.index_built = False

        # In production, initialize vector DB client here:
        # self.client = pinecone.Client(api_key=...)
        # self.index = self.client.Index("video-embeddings")

        logger.info(f"VectorSearchEngine initialized (STUBBED) with dim={embedding_dim}")

    def generate_embedding(self, video_id: str, metadata: Dict = None) -> np.ndarray:
        """
        Generate embedding for a video.

        In production, this would:
        1. Extract video features (title, description, visual features)
        2. Use a model (CLIP, VideoMAE, etc.) to generate embeddings

        Args:
            video_id: Video ID
            metadata: Video metadata for feature extraction

        Returns:
            Video embedding vector
        """
        # Stub: Generate random but consistent embedding for each video
        random.seed(hash(video_id) % 1000000)
        embedding = np.random.randn(self.embedding_dim)
        embedding = embedding / np.linalg.norm(embedding)  # Normalize

        return embedding

    def index_videos(self, video_ids: List[str], metadata_dict: Dict = None):
        """
        Index videos in the vector database.

        As per todo.md: This would be part of the background job that
        indexes new videos for semantic search.

        Args:
            video_ids: List of video IDs to index
            metadata_dict: Dictionary of video metadata
        """
        logger.info(f"Indexing {len(video_ids)} videos (STUBBED)")

        for video_id in video_ids:
            metadata = metadata_dict.get(video_id, {}) if metadata_dict else {}
            embedding = self.generate_embedding(video_id, metadata)
            self.video_embeddings[video_id] = VideoEmbedding(
                video_id=video_id,
                embedding=embedding,
                metadata=metadata
            )

        self.index_built = True

        # Production code would be:
        """
        vectors = []
        for video_id in video_ids:
            embedding = self.generate_embedding(video_id, metadata)
            vectors.append({
                "id": video_id,
                "values": embedding.tolist(),
                "metadata": metadata
            })

        self.index.upsert(vectors=vectors, batch_size=100)
        """

    def search_similar_videos(
        self,
        query_video_ids: List[str],
        limit: int = 100,
        exclude_ids: List[str] = None,
        diversity_factor: float = 0.3
    ) -> List[Tuple[str, float]]:
        """
        Search for similar videos based on query videos.

        As per todo.md: "High level idea for semantic search: have the success
        videos in a descending order of recency"

        Args:
            query_video_ids: Videos to find similar to (user's watch history)
            limit: Number of results to return
            exclude_ids: Video IDs to exclude from results
            diversity_factor: How much to diversify results (0-1)

        Returns:
            List of (video_id, similarity_score) tuples
        """
        if not self.index_built:
            logger.warning("Index not built, returning random videos")
            return self._get_random_videos(limit, exclude_ids)

        # Generate average query embedding from watch history
        query_embeddings = []
        for vid in query_video_ids[-10:]:  # Use last 10 watched videos
            if vid in self.video_embeddings:
                query_embeddings.append(self.video_embeddings[vid].embedding)

        if not query_embeddings:
            return self._get_random_videos(limit, exclude_ids)

        # Average embedding as query
        query_vector = np.mean(query_embeddings, axis=0)
        query_vector = query_vector / np.linalg.norm(query_vector)

        # Calculate similarities
        similarities = []
        exclude_set = set(exclude_ids or []) | set(query_video_ids)

        for vid, video_emb in self.video_embeddings.items():
            if vid in exclude_set:
                continue

            # Cosine similarity
            similarity = np.dot(query_vector, video_emb.embedding)

            # Apply diversity penalty if video is too similar to already selected
            if diversity_factor > 0 and similarities:
                max_sim_to_selected = max(
                    np.dot(video_emb.embedding, self.video_embeddings[s[0]].embedding)
                    for s in similarities[:5]
                ) if len(similarities) > 0 else 0

                similarity *= (1 - diversity_factor * max_sim_to_selected)

            similarities.append((vid, float(similarity)))

        # Sort by similarity and return top results
        similarities.sort(key=lambda x: x[1], reverse=True)

        # Production code would be:
        """
        results = self.index.query(
            vector=query_vector.tolist(),
            top_k=limit * 2,  # Fetch more for filtering
            filter={"video_id": {"$nin": exclude_ids}}
        )

        return [(match.id, match.score) for match in results.matches][:limit]
        """

        return similarities[:limit]

    def get_exploitation_videos(
        self,
        user_watch_history: List[str],
        limit: int = 100,
        max_iterations: int = 10
    ) -> List[str]:
        """
        Get exploitation/similarity videos for a user.

        As per todo.md: "pull the videos 10 times from vectorDB for the
        semantic search and finally stop"

        Args:
            user_watch_history: User's watched video IDs
            limit: Total videos to fetch
            max_iterations: Maximum search iterations

        Returns:
            List of recommended video IDs
        """
        logger.info(f"Getting exploitation videos for user with {len(user_watch_history)} watched videos")

        if not user_watch_history:
            return self._get_random_videos(limit, [])

        all_recommendations = []
        fetched_ids = set()
        batch_size = max(10, limit // max_iterations)

        for iteration in range(max_iterations):
            # Search for similar videos
            similar = self.search_similar_videos(
                query_video_ids=user_watch_history,
                limit=batch_size * 2,  # Fetch extra for filtering
                exclude_ids=list(fetched_ids)
            )

            # Add new videos
            for vid, score in similar:
                if vid not in fetched_ids:
                    all_recommendations.append(vid)
                    fetched_ids.add(vid)

                    if len(all_recommendations) >= limit:
                        break

            if len(all_recommendations) >= limit:
                break

            # For later iterations, focus on more recent history
            if iteration > 5:
                user_watch_history = user_watch_history[-20:]

        logger.info(f"Found {len(all_recommendations)} exploitation videos in {iteration + 1} iterations")
        return all_recommendations[:limit]

    def _get_random_videos(
        self,
        limit: int,
        exclude_ids: List[str] = None
    ) -> List[Tuple[str, float]]:
        """
        Get random videos as fallback.

        Args:
            limit: Number of videos
            exclude_ids: IDs to exclude

        Returns:
            List of (video_id, score) tuples
        """
        exclude_set = set(exclude_ids or [])
        available = [
            vid for vid in self.video_embeddings.keys()
            if vid not in exclude_set
        ]

        if not available:
            # Generate some dummy video IDs
            available = [f"vid_random_{i:06d}" for i in range(limit)]

        selected = random.sample(available, min(limit, len(available)))
        # Assign decreasing scores
        return [(vid, 1.0 - i * 0.01) for i, vid in enumerate(selected)]

    def update_user_profile(self, user_id: str, watched_videos: List[str]):
        """
        Update user's profile embedding based on watch history.

        This could be used for more personalized search.

        Args:
            user_id: User ID
            watched_videos: Recently watched videos
        """
        # In production, maintain user profile embeddings:
        """
        user_embeddings = []
        for vid in watched_videos[-50:]:  # Last 50 videos
            if vid in self.video_embeddings:
                user_embeddings.append(self.video_embeddings[vid])

        if user_embeddings:
            profile = np.mean(user_embeddings, axis=0)
            self.user_profiles[user_id] = profile
        """
        pass

    def get_trending_vectors(self, time_window: str = "1d") -> List[str]:
        """
        Get trending videos based on vector clustering.

        Videos that are being watched together form clusters.

        Args:
            time_window: Time window for trending

        Returns:
            List of trending video IDs
        """
        # Stub: Return some random videos
        all_videos = list(self.video_embeddings.keys())
        if all_videos:
            return random.sample(all_videos, min(50, len(all_videos)))
        return []


# Singleton instance
_engine = None

def get_vector_search_engine() -> VectorSearchEngine:
    """Get or create singleton vector search engine."""
    global _engine
    if _engine is None:
        _engine = VectorSearchEngine()

        # Pre-index some dummy videos for testing
        dummy_videos = [f"vid_{cat}_{i:06d}"
                       for cat in ["tech", "gaming", "music", "cooking"]
                       for i in range(100)]
        _engine.index_videos(dummy_videos)

    return _engine


if __name__ == "__main__":
    # Test the vector search engine
    logging.basicConfig(level=logging.INFO)

    engine = VectorSearchEngine()

    # Index some videos
    video_ids = [f"vid_test_{i:04d}" for i in range(100)]
    engine.index_videos(video_ids)

    # Search for similar videos
    watch_history = ["vid_test_0001", "vid_test_0002", "vid_test_0003"]
    similar = engine.search_similar_videos(watch_history, limit=10)
    print(f"Similar videos: {[v[0] for v in similar[:5]]}")

    # Get exploitation videos
    recommendations = engine.get_exploitation_videos(watch_history, limit=20)
    print(f"Exploitation recommendations: {recommendations[:10]}")