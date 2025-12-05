import chess.pgn
import time
import csv
import io
from multiprocessing import Pool, cpu_count, freeze_support
from tqdm import tqdm


path_to_data = "" # <-- Put pgn file here
num_of_games = 1000 # <-- Number of games to Process

def process_game(args):
    game_id, game_text = args
    game = chess.pgn.read_game(io.StringIO(game_text))
    board = game.board()
    move_num = 0
    rows = []
    white_elo = game.headers.get("WhiteElo")
    black_elo = game.headers.get("BlackElo")


    for move in game.mainline_moves():
        board.push(move)
        position = board.__str__().replace('\n', ' ').split()
        turn = move_num + 1
        game_moves = (move_num // 2) + 1

        for i, square in enumerate(position):
            if square != '.':

                rows.append([
                    game_id,
                    square,
                    turn,
                    8 - (i // 8),
                    (i % 8) + 1,
                    game_moves,
                    white_elo,
                    black_elo

                ])
        move_num += 1

    return rows

if __name__ == '__main__':
    freeze_support()
    start_time = time.time()

    data_path = path_to_data
    pgn = open(data_path, encoding='utf-8')

    games = []
    game_id = 1
    while game_id <= num_of_games:
        game = chess.pgn.read_game(pgn)
        if game is None:
            break
        games.append((game_id, str(game)))
        game_id += 1

    header = ['Game_id', 'piece', 'Turn', 'Row', 'Column', 'Move_Number', 'white_elo','black_elo']
    flush_threshold = 5000
    buffer = []

    with open('outputData.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)

        with Pool(processes=cpu_count()) as pool:
            for result in tqdm(pool.imap_unordered(process_game, games), total=len(games), desc="Processing games"):
                buffer.extend(result)
                if len(buffer) >= flush_threshold:
                    writer.writerows(buffer)
                    buffer.clear()

            if buffer:
                writer.writerows(buffer)

    end_time = time.time()
    print(f"Multiprocessing execution time: {end_time - start_time:.4f} seconds")