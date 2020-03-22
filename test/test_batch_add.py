import unittest
from rdflib.graph import Graph, BatchAddGraph
from rdflib.term import URIRef, Literal


class TestBatchAddGraph(unittest.TestCase):
    def test_buffer_size_zero_denied(self):
        with self.assertRaises(ValueError):
            BatchAddGraph(Graph(), buffer_size=0)

    def test_buffer_size_none_denied(self):
        with self.assertRaises(ValueError):
            BatchAddGraph(Graph(), buffer_size=None)

    def test_buffer_size_one_denied(self):
        with self.assertRaises(ValueError):
            BatchAddGraph(Graph(), buffer_size=1)

    def test_buffer_size_negative_denied(self):
        with self.assertRaises(ValueError):
            BatchAddGraph(Graph(), buffer_size=-12)

    def test_exit_submits_partial_batch(self):
        trip = (URIRef('a'), URIRef('b'), URIRef('c'))
        g = Graph()
        with BatchAddGraph(g, buffer_size=10) as cut:
            cut.add(trip)
        self.assertIn(trip, g)

    def test_add_more_than_buffer_size(self):
        trips = [(URIRef('a'), URIRef('b%d' % i), URIRef('c%d' % i))
                for i in range(12)]
        g = Graph()
        with BatchAddGraph(g, buffer_size=10) as cut:
            for trip in trips:
                cut.add(trip)
        self.assertEqual(12, len(g))

    def test_add_quad_for_non_conjunctive_empty(self):
        '''
        Graph drops quads that don't match our graph. Make sure we do the same
        '''
        g = Graph(identifier='http://example.org/g')
        badg = Graph(identifier='http://example.org/badness')
        with BatchAddGraph(g) as cut:
            cut.add((URIRef('a'), URIRef('b'), URIRef('c'), badg))
        self.assertEqual(0, len(g))

    def test_add_quad_for_non_conjunctive_pass_on_context_matches(self):
        g = Graph()
        with BatchAddGraph(g) as cut:
            cut.add((URIRef('a'), URIRef('b'), URIRef('c'), g))
        self.assertEqual(1, len(g))

    def test_no_addN_on_exception(self):
        '''
        Even if we've added triples so far, it may be that attempting to add the last
        batch is the cause of our exception, so we don't want to attempt again
        '''
        g = Graph()
        trips = [(URIRef('a'), URIRef('b%d' % i), URIRef('c%d' % i))
                for i in range(12)]

        try:
            with BatchAddGraph(g, buffer_size=10) as cut:
                for i, trip in enumerate(trips):
                    cut.add(trip)
                    if i == 11:
                        raise Exception('myexc')
        except Exception as e:
            if str(e) != 'myexc':
                pass
        self.assertEqual(10, len(g))

    def test_addN_batching_addN(self):
        class MockGraph(object):
            def __init__(self):
                self.counts = []

            def addN(self, quads):
                self.counts.append(sum(1 for _ in quads))

        g = MockGraph()
        quads = [(URIRef('a'), URIRef('b%d' % i), URIRef('c%d' % i), g)
                for i in range(12)]

        with BatchAddGraph(g, buffer_size=10, buffer_addn=True) as cut:
            cut.addN(quads)
        self.assertEqual(g.counts, [10, 2])

    def test_triples(self):
        g = Graph()
        trips = [(URIRef('a'), URIRef('b%d' % i), URIRef('c%d' % i), g)
                for i in range(12)]

        with BatchAddGraph(g, buffer_size=10) as cut:
            cut.addN(trips)
        self.assertEqual(set(t[:3] for t in trips), set(cut.triples((None, None, None))))

    def test_update(self):
        g = Graph()

        with BatchAddGraph(g, buffer_size=3, buffer_addn=True) as cut:
            cut.update('''
            PREFIX dc: <http://purl.org/dc/elements/1.1/>

            INSERT DATA
            {
              <http://example/book1> dc:title "A new book" .
              <http://example/book1> dc:title "Another book" .
              <http://example/book1> dc:title "NEW: BOOK!" .
              <http://example/book1> dc:title "Book 2: Electric Boogaloo" .
            }
            ''')
            self.assertEqual(len(g), 3)
        self.assertIn((URIRef('http://example/book1'),
                URIRef('http://purl.org/dc/elements/1.1/title'),
                Literal("Book 2: Electric Boogaloo")), g)

    def test_flush_flushes(self):
        g = Graph()

        cut = BatchAddGraph(g, buffer_size=2)
        cut.add((URIRef('a'), URIRef('b'), URIRef('c')))
        cut.flush()
        self.assertEqual(len(g), 1)

    def test_flush_clears_buffer(self):
        g = Graph()

        cut = BatchAddGraph(g, buffer_size=2)
        cut.add((URIRef('a'), URIRef('b'), URIRef('c')))
        cut.flush()
        self.assertEqual(len(cut._self_buffer), 0)

    def test_count(self):
        g = Graph()

        cut = BatchAddGraph(g, buffer_size=2)
        cut.add((URIRef('a'), URIRef('b'), URIRef('c')))
        self.assertEqual(cut.count, 1)
