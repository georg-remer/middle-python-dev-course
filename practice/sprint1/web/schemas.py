from marshmallow import Schema, fields


class ShortMovieSchema(Schema):
    id = fields.Str(required=True)
    title = fields.Str(required=True)
    imdb_rating = fields.Float()

    class Meta:
        ordered = True


class WriterSchema(Schema):
    id = fields.Str(required=True)
    name = fields.Str(required=True)

    class Meta:
        ordered = True


class ActorSchema(Schema):
    id = fields.Int(required=True)
    name = fields.Str(required=True)

    class Meta:
        ordered = True


class MovieSchema(Schema):
    id = fields.Str(required=True)
    title = fields.Str(required=True)
    description = fields.Str()
    imdb_rating = fields.Float()
    writers = fields.List(fields.Nested(WriterSchema))
    actors = fields.List(fields.Nested(ActorSchema))
    genre = fields.List(fields.Str())
    director = fields.List(fields.Str())

    class Meta:
        ordered = True
